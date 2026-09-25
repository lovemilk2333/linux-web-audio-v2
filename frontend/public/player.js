const DEFAULT_MAX_BUFFERED_SAMPLES = 24000; // 500 ms at 48 kHz
const FADE_SAMPLES = 256;
const SAMPLES_PER_FRAME = 480;

class PCMPlayerProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.packetQueue = [];
    this.packetTransitions = [];
    this.currentPacket = null;
    this.currentOffset = 0;
    this.bufferedSamples = 0;
    this.samplesSinceReport = 0;
    this.samplesPerPacket = 0;
    this.lowWatermarkSamples = SAMPLES_PER_FRAME;
    this.criticalWatermarkSamples = SAMPLES_PER_FRAME;
    this.prefillSamples = SAMPLES_PER_FRAME * 16;
    this.maxBufferedSamples = DEFAULT_MAX_BUFFERED_SAMPLES;
    this.highWatermarkSamples = DEFAULT_MAX_BUFFERED_SAMPLES;
    this.lowReportArmed = true;
    this.criticalReportArmed = true;
    this.highReportArmed = true;
    this.lastReceivedSequence = null;
    this.primed = false;
    this.fadeIn = 0;
    this.fadeOut = 0;
    this.crossfadeRemaining = 0;
    this.crossfadeTotal = 0;
    this.crossfadeStartLeft = 0;
    this.crossfadeStartRight = 0;
    this.needsCrossfade = false;
    this.resyncAfterUnderrun = false;
    this.lastLeft = 0;
    this.lastRight = 0;
    this.underruns = 0;
    this.droppedPackets = 0;
    this.resyncDroppedPackets = 0;

    this.port.onmessage = (event) => {
      const { type, interleaved, sequence, lowerFrames, targetFrames, upperFrames, discontinuity } = event.data;
      if (type === "SET_BUFFER_LIMIT") {
        this.lowWatermarkSamples = Math.max(1, lowerFrames) * SAMPLES_PER_FRAME;
        this.criticalWatermarkSamples = Math.max(1, Math.floor(Math.max(1, targetFrames) / 4)) * SAMPLES_PER_FRAME;
        this.prefillSamples = Math.max(1, targetFrames) * SAMPLES_PER_FRAME;
        this.highWatermarkSamples = Math.max(1, upperFrames || targetFrames) * SAMPLES_PER_FRAME;
        this.maxBufferedSamples = Math.max(
          DEFAULT_MAX_BUFFERED_SAMPLES,
          this.highWatermarkSamples,
        );
        this.trimBufferedSamples();
        this.lowReportArmed = this.bufferedSamples > this.lowWatermarkSamples;
        this.criticalReportArmed = this.bufferedSamples > this.criticalWatermarkSamples;
        this.highReportArmed = this.bufferedSamples <= this.prefillSamples;
        this.maybeReportLowBuffer();
        return;
      }
      if (type === "CLEAR") {
        this.packetQueue = [];
        this.packetTransitions = [];
        this.currentPacket = null;
        this.currentOffset = 0;
        this.bufferedSamples = 0;
        this.primed = false;
        this.fadeIn = 0;
        this.fadeOut = 0;
        this.crossfadeRemaining = 0;
        this.crossfadeTotal = 0;
        this.needsCrossfade = false;
        this.resyncAfterUnderrun = false;
        this.lastLeft = 0;
        this.lastRight = 0;
        this.lastReceivedSequence = null;
        this.lowReportArmed = true;
        this.criticalReportArmed = true;
        this.highReportArmed = true;
        this.underruns = 0;
        this.droppedPackets = 0;
        this.resyncDroppedPackets = 0;
        return;
      }
      if (type !== "PCM_DATA" || !interleaved || interleaved.length === 0 || interleaved.length % 2 !== 0) return;

      let packet = interleaved;
      if (this.resyncAfterUnderrun) {
        const targetSamples = Math.max(1, this.prefillSamples);
        const packetSamples = packet.length / 2;
        if (packetSamples > targetSamples) {
          packet = packet.subarray(packet.length - targetSamples * 2);
        }
      }

      this.packetQueue.push(packet);
      this.packetTransitions.push(Boolean(discontinuity));
      if (sequence !== null && sequence !== undefined) {
        this.lastReceivedSequence = sequence;
      }
      this.samplesPerPacket = packet.length / 2;
      this.bufferedSamples += this.samplesPerPacket;
      const exceededHighWatermark = this.bufferedSamples > this.highWatermarkSamples;
      if (this.resyncAfterUnderrun) {
        this.trimToLatestSamples(this.prefillSamples);
      }
      this.trimBufferedSamples();
      if (this.bufferedSamples > this.lowWatermarkSamples) this.lowReportArmed = true;
      if (this.bufferedSamples > this.criticalWatermarkSamples) this.criticalReportArmed = true;
      if (this.bufferedSamples <= this.prefillSamples) this.highReportArmed = true;
      this.maybeReportLowBuffer();
      if (exceededHighWatermark && this.highReportArmed) {
        this.highReportArmed = false;
        this.postBufferStatus(false, true);
      }
    };
  }

  trimBufferedSamples() {
    while (this.bufferedSamples > this.maxBufferedSamples && this.packetQueue.length > 1) {
      const discarded = this.packetQueue.shift();
      this.packetTransitions.shift();
      this.bufferedSamples -= discarded.length / 2;
      this.droppedPackets++;
      if (this.primed) this.needsCrossfade = true;
    }
  }

  trimToLatestSamples(limit) {
    while (this.bufferedSamples > limit && this.packetQueue.length > 0) {
      const excessSamples = this.bufferedSamples - limit;
      const oldest = this.packetQueue[0];
      const oldestSamples = oldest.length / 2;
      if (oldestSamples <= excessSamples) {
        this.packetQueue.shift();
        this.packetTransitions.shift();
        this.bufferedSamples -= oldestSamples;
        this.resyncDroppedPackets++;
        continue;
      }

      this.packetQueue[0] = oldest.subarray(excessSamples * 2);
      this.bufferedSamples -= excessSamples;
      break;
    }
  }

  startCrossfade(force = false) {
    if (!force && !this.needsCrossfade) return;
    this.crossfadeTotal = FADE_SAMPLES;
    this.crossfadeRemaining = FADE_SAMPLES;
    this.crossfadeStartLeft = this.lastLeft;
    this.crossfadeStartRight = this.lastRight;
    this.needsCrossfade = false;
  }

  postBufferStatus(lowWatermark = false, highWatermark = false, criticalWatermark = false) {
    this.port.postMessage({
      type: "BUFFER_STATUS",
      bufferedFrames: Math.ceil(this.bufferedSamples / SAMPLES_PER_FRAME),
      bufferedSamples: this.bufferedSamples,
      sequence: this.lastReceivedSequence,
      underruns: this.underruns,
      droppedPackets: this.droppedPackets,
      resyncDroppedPackets: this.resyncDroppedPackets,
      lowWatermark,
      criticalWatermark,
      highWatermark,
      primed: this.primed,
      resync: this.resyncAfterUnderrun,
    });
  }

  maybeReportLowBuffer() {
    if (this.lowReportArmed && this.lastReceivedSequence !== null && this.bufferedSamples <= this.lowWatermarkSamples) {
      this.lowReportArmed = false;
      this.postBufferStatus(true);
    }
    if (this.criticalReportArmed && this.lastReceivedSequence !== null && this.bufferedSamples <= this.criticalWatermarkSamples) {
      this.criticalReportArmed = false;
      this.postBufferStatus(false, false, true);
    }
  }

  process(inputs, outputs) {
    const output = outputs[0];
    const leftChannel = output[0];
    const rightChannel = output[1];
    if (!leftChannel || !rightChannel) return true;

    for (let i = 0; i < leftChannel.length; i++) {
      if (!this.primed && this.fadeOut === 0 && this.bufferedSamples >= this.prefillSamples) {
        this.primed = true;
        this.resyncAfterUnderrun = false;
        this.fadeIn = FADE_SAMPLES;
      }

      if (this.fadeOut > 0) {
        const scale = (this.fadeOut - 1) / FADE_SAMPLES;
        leftChannel[i] = this.lastLeft * scale;
        rightChannel[i] = this.lastRight * scale;
        this.fadeOut--;
        continue;
      }

      if (!this.primed) continue;

      if (!this.currentPacket) {
        this.currentPacket = this.packetQueue.shift() || null;
        const discontinuity = this.packetTransitions.shift() || false;
        this.currentOffset = 0;
        if (this.currentPacket) this.startCrossfade(discontinuity);
      }
      if (!this.currentPacket) {
        this.primed = false;
        this.fadeIn = 0;
        this.fadeOut = FADE_SAMPLES;
        this.resyncAfterUnderrun = true;
        this.packetQueue = [];
        this.packetTransitions = [];
        this.bufferedSamples = 0;
        this.underruns++;
        this.postBufferStatus();
        i--;
        continue;
      }

      const packet = this.currentPacket;
      let scale = this.fadeIn > 0
        ? Math.sin(((FADE_SAMPLES - this.fadeIn + 1) / FADE_SAMPLES) * Math.PI / 2)
        : 1;
      let left = packet[this.currentOffset] * scale;
      let right = packet[this.currentOffset + 1] * scale;
      if (this.crossfadeRemaining > 0) {
        const progress = (this.crossfadeTotal - this.crossfadeRemaining + 1) / this.crossfadeTotal;
        const crossfade = 0.5 - Math.cos(progress * Math.PI) * 0.5;
        left = this.crossfadeStartLeft * (1 - crossfade) + left * crossfade;
        right = this.crossfadeStartRight * (1 - crossfade) + right * crossfade;
        this.crossfadeRemaining--;
      }
      leftChannel[i] = left;
      rightChannel[i] = right;
      if (this.fadeIn > 0) this.fadeIn--;
      this.lastLeft = leftChannel[i];
      this.lastRight = rightChannel[i];
      this.currentOffset += 2;
      this.bufferedSamples--;

      if (this.currentOffset >= packet.length) {
        this.currentPacket = null;
      }
    }

    this.maybeReportLowBuffer();

    this.samplesSinceReport += leftChannel.length;
    if (this.samplesSinceReport >= 4800) {
      this.samplesSinceReport = 0;
      this.postBufferStatus();
    }
    return true;
  }
}

registerProcessor("pcm-player-processor", PCMPlayerProcessor);
