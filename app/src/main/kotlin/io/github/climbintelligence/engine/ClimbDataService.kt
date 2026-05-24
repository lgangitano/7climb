package io.github.climbintelligence.engine

import io.github.climbintelligence.ClimbIntelligenceExtension
import io.github.climbintelligence.data.model.ClimbInfo
import io.github.climbintelligence.data.model.ClimbSegment
import io.github.climbintelligence.data.model.LiveClimbState
import io.github.climbintelligence.data.model.NextClimbInfo
import io.github.climbintelligence.util.ElevationPolylineDecoder
import io.hammerhead.karooext.models.DataType
import io.hammerhead.karooext.models.OnNavigationState
import io.hammerhead.karooext.models.OnStreamState
import io.hammerhead.karooext.models.StreamState
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicReference

class ClimbDataService(private val climbExtension: ClimbIntelligenceExtension) {

    companion object {
        private const val TAG = "ClimbDataService"
    }

    private val _liveState = MutableStateFlow(LiveClimbState())
    val liveState: StateFlow<LiveClimbState> = _liveState.asStateFlow()

    private val _activeClimb = MutableStateFlow<ClimbInfo?>(null)
    val activeClimb: StateFlow<ClimbInfo?> = _activeClimb.asStateFlow()

    /** All climbs on the current route, parsed from NavigationState */
    private val _routeClimbs = MutableStateFlow<List<ClimbInfo>>(emptyList())
    val routeClimbs: StateFlow<List<ClimbInfo>> = _routeClimbs.asStateFlow()

    /** Whether a route with climbs is currently loaded */
    private val _hasRoute = MutableStateFlow(false)
    val hasRoute: StateFlow<Boolean> = _hasRoute.asStateFlow()

    /** Next upcoming climb on the route */
    private val _nextClimb = MutableStateFlow(NextClimbInfo())
    val nextClimb: StateFlow<NextClimbInfo> = _nextClimb.asStateFlow()

    // Consumer IDs for cleanup
    private val powerConsumerId = AtomicReference<String?>(null)
    private val hrConsumerId = AtomicReference<String?>(null)
    private val cadenceConsumerId = AtomicReference<String?>(null)
    private val speedConsumerId = AtomicReference<String?>(null)
    private val elevationGainConsumerId = AtomicReference<String?>(null)
    private val gradeConsumerId = AtomicReference<String?>(null)
    private val distanceConsumerId = AtomicReference<String?>(null)
    private val locationConsumerId = AtomicReference<String?>(null)
    private val navigationConsumerId = AtomicReference<String?>(null)
    // Karoo CLIMB stream (karoo-ext 1.1.8+) — native Climber detection on routes + freestyle
    private val climbStreamConsumerId = AtomicReference<String?>(null)
    private val climbNumberConsumerId = AtomicReference<String?>(null)

    // Current values (thread-safe via AtomicReference)
    private val currentPower = AtomicReference(0)
    private val currentHR = AtomicReference(0)
    private val currentCadence = AtomicReference(0)
    private val currentSpeed = AtomicReference(0.0)
    private val currentAltitude = AtomicReference(0.0)
    private val currentGrade = AtomicReference(0.0)
    private val currentDistance = AtomicReference(0.0)
    private val currentLat = AtomicReference(0.0)
    private val currentLon = AtomicReference(0.0)

    // CLIMB stream cached fields (last emitted values)
    private val currentClimbDistanceToTop = AtomicReference(0.0)
    private val currentClimbElevationToTop = AtomicReference(0.0)
    private val currentClimbDistanceFromBottom = AtomicReference(0.0)
    private val currentClimbElevationFromBottom = AtomicReference(0.0)
    private val currentClimbElevationTotal = AtomicReference(0.0)
    private val currentClimbNumber = AtomicReference(0)
    private val lastClimbStreamEmitMs = AtomicReference(0L)
    private val climbStreamStartTimestamp = AtomicReference(0L)

    /** True while the Karoo CLIMB stream is emitting on an active climb. */
    private val _climbStreamActive = MutableStateFlow(false)
    val climbStreamActive: StateFlow<Boolean> = _climbStreamActive.asStateFlow()

    @Volatile
    private var hasReceivedData = false

    /** Set by sensor callbacks, cleared by 1Hz timer after emission.
     *  Prevents stale emissions when sensors are paused (e.g. ride pause). */
    @Volatile
    private var sensorUpdatedSinceLastEmit = false

    private var emitJob: Job? = null

    // Cached route elevation profile points
    @Volatile
    private var routeElevationPoints: List<ElevationPolylineDecoder.ElevationPoint> = emptyList()

    fun startStreaming() {
        android.util.Log.i(TAG, "Starting data stream subscriptions")
        emitJob?.cancel() // Defensive: prevent duplicate timers on reconnect

        try {
            // --- Navigation state subscription ---
            navigationConsumerId.set(
                climbExtension.karooSystem.addConsumer<OnNavigationState> { event ->
                    handleNavigationState(event.state)
                }
            )

            // --- Sensor data subscriptions ---
            powerConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.POWER)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.toInt()?.let { value ->
                            currentPower.set(value)
                            hasReceivedData = true
                            sensorUpdatedSinceLastEmit = true
                        }
                    }
                }
            )

            hrConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.HEART_RATE)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.toInt()?.let { value ->
                            currentHR.set(value)
                            sensorUpdatedSinceLastEmit = true
                        }
                    }
                }
            )

            cadenceConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.CADENCE)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.toInt()?.let { value ->
                            currentCadence.set(value)
                            sensorUpdatedSinceLastEmit = true
                        }
                    }
                }
            )

            speedConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.SPEED)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.let { value ->
                            currentSpeed.set(value)
                            sensorUpdatedSinceLastEmit = true
                        }
                    }
                }
            )

            elevationGainConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.ELEVATION_GAIN)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.let { value ->
                            currentAltitude.set(value)
                            sensorUpdatedSinceLastEmit = true
                        }
                    }
                }
            )

            gradeConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.ELEVATION_GRADE)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.let { value ->
                            currentGrade.set(value)
                            sensorUpdatedSinceLastEmit = true
                        }
                    }
                }
            )

            distanceConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.DISTANCE)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.singleValue?.let { value ->
                            currentDistance.set(value)
                            sensorUpdatedSinceLastEmit = true
                            updateActiveClimbFromRoute()
                        }
                    }
                }
            )

            locationConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.LOCATION)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        val values = state.dataPoint.values
                        values["lat"]?.let { currentLat.set(it) }
                        values["lng"]?.let { currentLon.set(it) }
                        sensorUpdatedSinceLastEmit = true
                    }
                }
            )

            // --- Karoo CLIMB stream (1.1.8+) ---
            // Compound DataType carrying DISTANCE_FROM_BOTTOM, DISTANCE_TO_TOP,
            // ELEVATION_FROM_BOTTOM, ELEVATION_TO_TOP, CLIMB_ELEVATION — driven by
            // Karoo's native Climber detection on routes AND freestyle rides.
            climbStreamConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.CLIMB)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        val values = state.dataPoint.values
                        values[DataType.Field.DISTANCE_TO_TOP]?.let { currentClimbDistanceToTop.set(it) }
                        values[DataType.Field.DISTANCE_FROM_BOTTOM]?.let { currentClimbDistanceFromBottom.set(it) }
                        values[DataType.Field.ELEVATION_TO_TOP]?.let { currentClimbElevationToTop.set(it) }
                        values[DataType.Field.ELEVATION_FROM_BOTTOM]?.let { currentClimbElevationFromBottom.set(it) }
                        values[DataType.Field.CLIMB_ELEVATION]?.let { currentClimbElevationTotal.set(it) }
                        lastClimbStreamEmitMs.set(System.currentTimeMillis())

                        val onClimb = currentClimbDistanceToTop.get() > 0.0 ||
                                      currentClimbDistanceFromBottom.get() > 0.0
                        val wasActive = _climbStreamActive.value
                        _climbStreamActive.value = onClimb

                        if (onClimb && !wasActive) {
                            climbStreamStartTimestamp.set(System.currentTimeMillis())
                            android.util.Log.i(
                                TAG,
                                "CLIMB stream activated — top in ${currentClimbDistanceToTop.get().toInt()}m" +
                                    " (${currentClimbElevationToTop.get().toInt()}m elevation)"
                            )
                        } else if (!onClimb && wasActive) {
                            android.util.Log.i(TAG, "CLIMB stream ended")
                        }

                        updateActiveClimbFromStream()
                        sensorUpdatedSinceLastEmit = true
                    }
                }
            )

            climbNumberConsumerId.set(
                climbExtension.karooSystem.addConsumer(
                    OnStreamState.StartStreaming(DataType.Type.CLIMB_NUMBER)
                ) { event: OnStreamState ->
                    val state = event.state
                    if (state is StreamState.Streaming) {
                        state.dataPoint.values[DataType.Field.CLIMB_NUMBER]?.toInt()?.let {
                            currentClimbNumber.set(it)
                        }
                    }
                }
            )
        } catch (e: Exception) {
            android.util.Log.e(TAG, "Failed to start streaming: ${e.message}")
        }

        // Single 1Hz emission timer — replaces per-callback emitState() calls.
        // Only emits when at least one sensor has fired since the last emission,
        // preventing stale data accumulation during ride pause.
        emitJob = CoroutineScope(Dispatchers.Main.immediate).launch {
            while (isActive) {
                delay(1000)
                // Watchdog: deactivate CLIMB stream if no emit in the last 5s
                // (Karoo sometimes stops emitting without an explicit zero-out
                // when a climb ends; without this the activeClimb would stick).
                if (_climbStreamActive.value &&
                    (System.currentTimeMillis() - lastClimbStreamEmitMs.get()) > 5000L
                ) {
                    android.util.Log.i(TAG, "CLIMB stream watchdog: no emit in 5s — deactivating")
                    _climbStreamActive.value = false
                    updateActiveClimbFromStream()
                }
                if (hasReceivedData && sensorUpdatedSinceLastEmit) {
                    sensorUpdatedSinceLastEmit = false
                    emitState()
                }
            }
        }
    }

    // ── Navigation handling ──────────────────────────────────────────────

    private fun handleNavigationState(state: OnNavigationState.NavigationState) {
        when (state) {
            is OnNavigationState.NavigationState.NavigatingRoute -> {
                android.util.Log.i(TAG, "Route loaded: ${state.name}, ${state.climbs.size} climbs")
                _hasRoute.value = true

                // Decode elevation polyline for segment analysis
                routeElevationPoints = state.routeElevationPolyline?.let { polyline ->
                    when (val result = ElevationPolylineDecoder.decodeSafe(polyline)) {
                        is ElevationPolylineDecoder.DecodeResult.Success -> {
                            android.util.Log.i(TAG, "Decoded ${result.points.size} elevation points")
                            ElevationPolylineDecoder.smooth(result.points)
                        }
                        is ElevationPolylineDecoder.DecodeResult.Error -> {
                            android.util.Log.w(TAG, "Elevation decode failed: ${result.message}")
                            emptyList()
                        }
                    }
                } ?: emptyList()

                // Convert Karoo climbs to our ClimbInfo model
                val climbs = state.climbs.mapIndexed { index, karooClimb ->
                    buildClimbInfo(index, karooClimb)
                }
                _routeClimbs.value = climbs

                // Set first upcoming climb as active if none is set
                if (_activeClimb.value == null && climbs.isNotEmpty()) {
                    val dist = currentDistance.get()
                    val upcoming = climbs.firstOrNull { dist < it.startDistance + it.length }
                    if (upcoming != null) {
                        _activeClimb.value = upcoming.copy(isActive = false)
                    }
                }
            }

            is OnNavigationState.NavigationState.NavigatingToDestination -> {
                _hasRoute.value = true
                routeElevationPoints = state.elevationPolyline?.let { polyline ->
                    when (val result = ElevationPolylineDecoder.decodeSafe(polyline)) {
                        is ElevationPolylineDecoder.DecodeResult.Success ->
                            ElevationPolylineDecoder.smooth(result.points)
                        is ElevationPolylineDecoder.DecodeResult.Error -> emptyList()
                    }
                } ?: emptyList()

                val climbs = state.climbs.mapIndexed { index, karooClimb ->
                    buildClimbInfo(index, karooClimb)
                }
                _routeClimbs.value = climbs
            }

            is OnNavigationState.NavigationState.Idle -> {
                android.util.Log.i(TAG, "Navigation idle — no route")
                _hasRoute.value = false
                _routeClimbs.value = emptyList()
                routeElevationPoints = emptyList()
                // Don't clear activeClimb — ClimbDetector may provide detected climbs
            }
        }
    }

    /**
     * Convert a Karoo NavigationState.Climb into our ClimbInfo model,
     * including segment breakdown from elevation polyline.
     */
    private fun buildClimbInfo(index: Int, karooClimb: OnNavigationState.NavigationState.Climb): ClimbInfo {
        val climbId = "route_${index}_${karooClimb.startDistance.toInt()}"

        // Extract segments from elevation profile if available
        val segments = if (routeElevationPoints.isNotEmpty()) {
            val climbProfile = ElevationPolylineDecoder.extractClimbProfile(
                routeElevationPoints,
                karooClimb.startDistance,
                karooClimb.length
            )
            ElevationPolylineDecoder.buildSegments(climbProfile, karooClimb.length)
                .map { seg ->
                    ClimbSegment(
                        startDistance = seg.startDistance,
                        endDistance = seg.endDistance,
                        grade = seg.grade,
                        length = seg.length,
                        elevation = seg.elevation
                    )
                }
        } else {
            // No polyline — create single segment from average data
            listOf(
                ClimbSegment(
                    startDistance = 0.0,
                    endDistance = karooClimb.length,
                    grade = karooClimb.grade,
                    length = karooClimb.length,
                    elevation = karooClimb.totalElevation
                )
            )
        }

        val maxGrade = segments.maxOfOrNull { it.grade } ?: karooClimb.grade

        val category = categorizeClimb(
            karooClimb.length,
            karooClimb.totalElevation,
            karooClimb.grade
        )

        // Generate descriptive name since Karoo doesn't provide climb names
        val catLabel = when (category) {
            1 -> "HC"; 2 -> "Cat 1"; 3 -> "Cat 2"; 4 -> "Cat 3"; else -> "Cat 4"
        }
        val lengthKm = "%.1fkm".format(karooClimb.length / 1000.0)
        val climbName = "$catLabel $lengthKm"

        return ClimbInfo(
            id = climbId,
            name = climbName,
            category = category,
            length = karooClimb.length,
            elevation = karooClimb.totalElevation,
            avgGrade = karooClimb.grade,
            maxGrade = maxGrade,
            segments = segments,
            distanceToTop = karooClimb.length,
            elevationToTop = karooClimb.totalElevation,
            progress = 0.0,
            isActive = false,
            isFromRoute = true,
            startDistance = karooClimb.startDistance
        )
    }

    /**
     * Called on every distance update — checks if rider is on a route climb
     * and updates activeClimb with live progress metrics. When the Karoo CLIMB
     * stream is firing (karoo-ext 1.1.8+), it owns activeClimb's progress fields
     * and this method skips that write; the nextClimb countdown stays computed
     * here from route position regardless of stream state.
     */
    private fun updateActiveClimbFromRoute() {
        val climbs = _routeClimbs.value
        if (climbs.isEmpty()) return

        val dist = currentDistance.get()
        val streamOwnsActiveClimb = _climbStreamActive.value

        // Find the climb we're currently on
        val onClimb = climbs.firstOrNull { climb ->
            dist >= climb.startDistance && dist < (climb.startDistance + climb.length)
        }

        if (onClimb != null) {
            if (!streamOwnsActiveClimb) {
                val distOnClimb = dist - onClimb.startDistance
                val distToTop = onClimb.length - distOnClimb
                val progress = (distOnClimb / onClimb.length).coerceIn(0.0, 1.0)
                val elevToTop = onClimb.elevation * (distToTop / onClimb.length)

                _activeClimb.value = onClimb.copy(
                    distanceToTop = distToTop,
                    elevationToTop = elevToTop,
                    progress = progress,
                    isActive = true
                )
            }

            // While on a climb, look for the next one after this climb
            val nextAfterCurrent = climbs.firstOrNull { it.startDistance > onClimb.startDistance + onClimb.length }
            if (nextAfterCurrent != null) {
                val distToNext = (nextAfterCurrent.startDistance - dist).coerceAtLeast(0.0)
                val speed = currentSpeed.get()
                val eta = if (speed > 0.5) (distToNext / speed).toLong() else 0L
                _nextClimb.value = NextClimbInfo(
                    distanceToStart = distToNext,
                    etaSeconds = eta,
                    climbName = nextAfterCurrent.name,
                    climbCategory = nextAfterCurrent.category,
                    climbLength = nextAfterCurrent.length,
                    climbElevation = nextAfterCurrent.elevation,
                    climbAvgGrade = nextAfterCurrent.avgGrade,
                    hasNext = true
                )
            } else {
                _nextClimb.value = NextClimbInfo()
            }
        } else {
            // Not on a climb — show next upcoming climb (if any)
            val next = climbs.firstOrNull { it.startDistance > dist }
            if (next != null) {
                if (!streamOwnsActiveClimb) {
                    _activeClimb.value = next.copy(
                        distanceToTop = next.length,
                        elevationToTop = next.elevation,
                        progress = 0.0,
                        isActive = false
                    )
                }

                // Update next climb countdown
                val distToNext = (next.startDistance - dist).coerceAtLeast(0.0)
                val speed = currentSpeed.get()
                val eta = if (speed > 0.5) (distToNext / speed).toLong() else 0L
                _nextClimb.value = NextClimbInfo(
                    distanceToStart = distToNext,
                    etaSeconds = eta,
                    climbName = next.name,
                    climbCategory = next.category,
                    climbLength = next.length,
                    climbElevation = next.elevation,
                    climbAvgGrade = next.avgGrade,
                    hasNext = true
                )
            } else {
                if (!streamOwnsActiveClimb && _activeClimb.value?.isFromRoute == true) {
                    // Past all route climbs
                    _activeClimb.value = null
                }
                _nextClimb.value = NextClimbInfo()
            }
        }
    }

    /**
     * Build _activeClimb from the Karoo CLIMB stream cached fields. Called from
     * the CLIMB consumer callback (karoo-ext 1.1.8+). When a route climb is
     * underfoot, route metadata (name, category, segments) is overlaid with the
     * stream's authoritative distanceToTop / elevationToTop / progress.
     * Otherwise a stream-only ClimbInfo is synthesized — no segments means
     * Layer 2 (route strategy) and Layer 3 (tactical analyzer) won't fire,
     * but Layer 1 pacing target still works off the median grade.
     */
    private fun updateActiveClimbFromStream() {
        if (!_climbStreamActive.value) {
            // Stream just deactivated — clear stream-driven climb (route-only
            // climbs are managed by updateActiveClimbFromRoute).
            val current = _activeClimb.value
            if (current != null && !current.isFromRoute) {
                _activeClimb.value = null
            }
            return
        }

        val distToTop = currentClimbDistanceToTop.get()
        val distFromBottom = currentClimbDistanceFromBottom.get()
        val elevToTop = currentClimbElevationToTop.get()
        val elevFromBottom = currentClimbElevationFromBottom.get()
        val climbElevationTotal = currentClimbElevationTotal.get()
        val totalLength = distToTop + distFromBottom
        val totalElevation = if (climbElevationTotal > 0.0) climbElevationTotal
            else (elevToTop + elevFromBottom)
        val progress = if (totalLength > 0.0)
            (distFromBottom / totalLength).coerceIn(0.0, 1.0) else 0.0
        val avgGrade = if (totalLength > 0.0)
            (totalElevation / totalLength) * 100.0 else 0.0

        val dist = currentDistance.get()
        val routeClimb = _routeClimbs.value.firstOrNull { rc ->
            dist >= rc.startDistance && dist < (rc.startDistance + rc.length)
        }

        _activeClimb.value = if (routeClimb != null) {
            // Route climb metadata + stream's authoritative progress fields
            routeClimb.copy(
                distanceToTop = distToTop,
                elevationToTop = elevToTop,
                progress = progress,
                isActive = true
            )
        } else {
            // No route — synthesize from stream alone (no segments / category)
            val climbNum = currentClimbNumber.get()
            val startTs = climbStreamStartTimestamp.get()
                .takeIf { it > 0 } ?: System.currentTimeMillis()
            val lengthKm = "%.1f km".format(totalLength / 1000.0)
            ClimbInfo(
                id = "karoo_climb_${climbNum}_${startTs}",
                name = "Climb $climbNum ($lengthKm)",
                category = 0,
                length = totalLength,
                elevation = totalElevation,
                avgGrade = avgGrade,
                maxGrade = avgGrade,
                segments = emptyList(),
                distanceToTop = distToTop,
                elevationToTop = elevToTop,
                progress = progress,
                isActive = true,
                isFromRoute = false,
                startTimestamp = startTs
            )
        }
    }

    /**
     * Categorize a climb by difficulty: 1=HC, 2=Cat1, 3=Cat2, 4=Cat3, 5=Cat4
     */
    private fun categorizeClimb(length: Double, elevation: Double, grade: Double): Int {
        val score = elevation * grade // Simple climb score
        return when {
            score > 8000 || (elevation > 1000 && grade > 7) -> 1   // HC
            score > 4000 || (elevation > 600 && grade > 6)  -> 2   // Cat 1
            score > 2000 || (elevation > 400 && grade > 5)  -> 3   // Cat 2
            score > 1000 || (elevation > 200 && grade > 4)  -> 4   // Cat 3
            else                                             -> 5   // Cat 4
        }
    }

    // ── State emission ───────────────────────────────────────────────────

    private fun emitState() {
        _liveState.value = LiveClimbState(
            power = currentPower.get(),
            heartRate = currentHR.get(),
            cadence = currentCadence.get(),
            speed = currentSpeed.get(),
            altitude = currentAltitude.get(),
            grade = currentGrade.get(),
            distance = currentDistance.get(),
            latitude = currentLat.get(),
            longitude = currentLon.get(),
            timestamp = System.currentTimeMillis(),
            hasData = hasReceivedData
        )
    }

    fun updateActiveClimb(climb: ClimbInfo?) {
        _activeClimb.value = climb
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    fun stopStreaming() {
        android.util.Log.i(TAG, "Stopping data stream subscriptions")
        emitJob?.cancel()
        emitJob = null
        removeConsumer(powerConsumerId)
        removeConsumer(hrConsumerId)
        removeConsumer(cadenceConsumerId)
        removeConsumer(speedConsumerId)
        removeConsumer(elevationGainConsumerId)
        removeConsumer(gradeConsumerId)
        removeConsumer(distanceConsumerId)
        removeConsumer(locationConsumerId)
        removeConsumer(navigationConsumerId)
        removeConsumer(climbStreamConsumerId)
        removeConsumer(climbNumberConsumerId)
        _climbStreamActive.value = false
    }

    private fun removeConsumer(ref: AtomicReference<String?>) {
        ref.getAndSet(null)?.let { id ->
            try {
                climbExtension.karooSystem.removeConsumer(id)
            } catch (e: Exception) {
                android.util.Log.w(TAG, "Failed to remove consumer: ${e.message}")
            }
        }
    }

    fun destroy() {
        stopStreaming()
    }
}
