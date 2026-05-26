package io.github.climbintelligence.engine

import io.github.climbintelligence.data.PreferencesRepository
import io.github.climbintelligence.data.model.AthleteProfile
import io.github.climbintelligence.data.model.LiveClimbState
import io.github.climbintelligence.data.model.WPrimeSample
import io.github.climbintelligence.data.model.WPrimeState
import io.github.climbintelligence.data.model.WPrimeStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/**
 * Skiba differential W' Balance model.
 *
 * dW'/dt = recovery - expenditure
 * if P > CP: dW' = ((W'max - W') / tau - (P - CP)) * dt
 * if P <= CP: dW' = ((W'max - W') / tau) * dt
 * tau = 546.0 (Skiba constant)
 *
 * As of the W'-history-chart PR, balance is allowed to be negative — the
 * Skiba math predicts depletion past zero whenever power > CP for long
 * enough, and that signal is the model's way of saying "W'max is calibrated
 * too low for this rider." A sanity floor at -wMax keeps a stuck-high sensor
 * from driving the value to absurd negatives.
 */
class WPrimeEngine(private val preferencesRepository: PreferencesRepository) {

    companion object {
        private const val TAG = "WPrimeEngine"
        private const val TAU = 546.0 // Skiba recovery time constant
        private const val DT = 1.0    // 1 second update interval
        /** Ring-buffer cap for per-tick history. 86400 = 24h at 1Hz, ~2.8 MB. */
        const val MAX_HISTORY_SAMPLES = 86400
    }

    private val _state = MutableStateFlow(WPrimeState())
    val state: StateFlow<WPrimeState> = _state.asStateFlow()

    /**
     * Per-tick W' history for the current ride. Append-on-update, ring-buffered
     * at MAX_HISTORY_SAMPLES. Cleared on reset(); NOT restored across crashes
     * (CheckpointManager restores current balance but not history — chart
     * resumes from the current value after a restart).
     */
    private val _wPrimeHistory = MutableStateFlow<List<WPrimeSample>>(emptyList())
    val wPrimeHistory: StateFlow<List<WPrimeSample>> = _wPrimeHistory.asStateFlow()

    private val historyBuffer = ArrayDeque<WPrimeSample>()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private var wBalance: Double = 20000.0
    private var wMax: Double = 20000.0
    private var cp: Int = 0
    private var lastUpdateTime: Long = 0L
    private var profileLoaded = false

    /**
     * True while the Karoo ride is in [io.hammerhead.karooext.models.RideState.Paused].
     * Set by [RideStateMonitor]. When paused, [update] is short-circuited and the
     * engine is driven instead by [tickPauseRecovery] at 1 Hz — pause time
     * accumulates toward W' refill at the same rate as coasting (Skiba P ≤ CP
     * recovery branch with P = 0).
     */
    @Volatile
    private var pauseActive: Boolean = false

    init {
        scope.launch {
            preferencesRepository.athleteProfileFlow.collect { profile ->
                if (profile.isConfigured) {
                    val newMax = profile.wPrimeMax.toDouble()
                    cp = profile.effectiveCp
                    if (!profileLoaded) {
                        wMax = newMax
                        wBalance = newMax
                        profileLoaded = true
                    } else {
                        wMax = newMax
                        // Clamp balance if W'max decreased mid-ride (prevents >100%)
                        wBalance = wBalance.coerceAtMost(wMax)
                    }
                }
            }
        }
    }

    fun setPaused(paused: Boolean) {
        pauseActive = paused
    }

    fun update(state: LiveClimbState) {
        if (!profileLoaded || cp == 0) return
        // While the ride is paused, the recovery ticker owns the engine —
        // skip real samples so we don't double-count pause time.
        if (pauseActive) return
        // Gate ONLY on the presence of sensor data, not on power being non-zero.
        // power == 0 is a legitimate observation (rider coasting, recovery phase) —
        // the Skiba math handles it correctly (recovery branch applies). The prior
        // code's early-return on power == 0 froze the model AND the history during
        // coasting, hiding the recovery curve and leaving the chart empty for
        // riders who hadn't yet pedaled at full power.
        if (!state.hasData) return

        val now = state.timestamp
        val power = state.power.toDouble()
        val recovery = (wMax - wBalance) / TAU

        // Compute balance delta only when we have a previous timestamp to diff
        // against. On the very first tick we just seed lastUpdateTime + record
        // the baseline sample — no dW, no compute, but the chart gets its
        // first data point immediately rather than starting one tick late.
        if (lastUpdateTime != 0L) {
            val rawDt = (now - lastUpdateTime) / 1000.0
            // Clamp dt: minimum 0.1s prevents freeze on same-timestamp events,
            // maximum 5s caps large gaps (e.g. after pause)
            val dt = rawDt.coerceIn(0.1, 5.0)

            val dW = if (power > cp) {
                (recovery - (power - cp)) * dt
            } else {
                recovery * dt
            }

            // Allow negative — that's the model's signal that W'max is calibrated too low.
            // Cap above at wMax (can't exceed the model's max capacity) and floor at -wMax
            // (sanity bound — a stuck-high sensor can't drift to absurd negatives).
            wBalance = (wBalance + dW).coerceIn(-wMax, wMax)
        }
        lastUpdateTime = now

        val pct = wBalance / wMax * 100.0
        val depletionRate = if (power > cp) (power - cp) else 0.0
        val recoveryRateVal = if (power <= cp) recovery else 0.0

        // timeToEmpty only meaningful when balance is positive and depleting
        val timeToEmpty = if (wBalance > 0 && depletionRate > recoveryRateVal && depletionRate > 0) {
            (wBalance / (depletionRate - recoveryRateVal)).toLong().coerceAtMost(3600L)
        } else -1L

        val timeToFull = if (recoveryRateVal > 0 && wBalance < wMax) {
            ((wMax - wBalance) / recoveryRateVal).toLong().coerceAtMost(3600L)
        } else -1L

        _state.value = WPrimeState(
            balance = wBalance,
            maxBalance = wMax,
            percentage = pct,
            depletionRate = depletionRate,
            recoveryRate = recoveryRateVal,
            timeToEmpty = timeToEmpty,
            timeToFull = timeToFull,
            status = WPrimeState.statusForPercentage(pct)
        )

        // Append to history every tick where sensor data is flowing — including
        // coasting, including the very first tick (baseline). Drives the W'
        // history chart's continuous live view across the entire ride.
        recordHistorySample(now, wBalance, pct, state.power)
    }

    /**
     * Apply one second of pure recovery — pause-time accounting.
     *
     * Driven by [RideStateMonitor]'s 1 Hz ticker while the Karoo ride is in
     * Paused state, where no real power samples flow but Skiba's recovery
     * branch still applies (P = 0 ≤ CP). Pause time accumulates toward
     * W' refill at the same rate as coasting on the bike.
     *
     * Same invariants as [update]: profile-gated, dt clamped, balance kept
     * in [-wMax, wMax], lastUpdateTime advanced, history sample appended at
     * power = 0 so the chart shows a continuous curve through pause.
     */
    fun tickPauseRecovery(now: Long) {
        if (!profileLoaded || cp == 0) return
        if (!pauseActive) return // belt-and-suspenders; only run while paused

        val recovery = (wMax - wBalance) / TAU

        if (lastUpdateTime != 0L) {
            val rawDt = (now - lastUpdateTime) / 1000.0
            val dt = rawDt.coerceIn(0.1, 5.0)
            wBalance = (wBalance + recovery * dt).coerceIn(-wMax, wMax)
        }
        lastUpdateTime = now

        val pct = wBalance / wMax * 100.0
        val recoveryRateVal = (wMax - wBalance) / TAU
        val timeToFull = if (recoveryRateVal > 0 && wBalance < wMax) {
            ((wMax - wBalance) / recoveryRateVal).toLong().coerceAtMost(3600L)
        } else -1L

        _state.value = WPrimeState(
            balance = wBalance,
            maxBalance = wMax,
            percentage = pct,
            depletionRate = 0.0,
            recoveryRate = recoveryRateVal,
            timeToEmpty = -1L,
            timeToFull = timeToFull,
            status = WPrimeState.statusForPercentage(pct)
        )

        recordHistorySample(now, wBalance, pct, power = 0)
    }

    private fun recordHistorySample(
        timestampMs: Long,
        balance: Double,
        percentage: Double,
        power: Int
    ) {
        historyBuffer.addLast(
            WPrimeSample(
                timestampMs = timestampMs,
                balance = balance,
                percentage = percentage,
                power = power
            )
        )
        while (historyBuffer.size > MAX_HISTORY_SAMPLES) {
            historyBuffer.removeFirst()
        }
        // Publish an immutable snapshot to the StateFlow. ArrayList copy keeps
        // collectors safe from concurrent mutation of historyBuffer.
        _wPrimeHistory.value = ArrayList(historyBuffer)
    }

    fun reset() {
        wBalance = wMax
        lastUpdateTime = 0L
        historyBuffer.clear()
        _wPrimeHistory.value = emptyList()
        _state.value = WPrimeState(balance = wMax, maxBalance = wMax)
    }

    /**
     * Restore current balance from a checkpoint. History is NOT restored —
     * the chart resumes from this point with no prior history shown. A future
     * `chore/restore-history-from-fit` PR could read the in-progress FIT
     * file to reconstruct samples up to the crash; out of scope here.
     */
    fun restore(balance: Double) {
        wBalance = balance.coerceIn(-wMax, wMax)
        _state.value = WPrimeState.fromBalance(wBalance, wMax)
    }
}
