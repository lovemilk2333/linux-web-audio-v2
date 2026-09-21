// player.js - 无缓冲区、即收即播模式

class PCMPlayerProcessor extends AudioWorkletProcessor {
  constructor() {
    super();

    // 存放接收到的 raw 交错数据帧队列 [Float32Array, Float32Array, ...]
    this.packetQueue = [];
    
    // 当前正在消费的 packet 及其内部读取偏移指针
    this.currentPacket = null;
    this.currentOffset = 0;

    this.port.onmessage = (event) => {
      const { type, interleaved } = event.data;
      if (type === "PCM_DATA" && interleaved) {
        // 直接压入队列，给什么写什么
        this.packetQueue.push(interleaved);
      }
    };
  }

  process(inputs, outputs) {
    const output = outputs[0];
    const leftChannel = output[0];  // 声道 0: 左
    const rightChannel = output[1]; // 声道 1: 右

    if (!leftChannel || !rightChannel) return true;

    const frameSize = leftChannel.length; // 通常为 128 点

    // 逐点填充当前 128 点的输出帧
    for (let i = 0; i < frameSize; i++) {
      // 如果当前没有正在读取的 packet，尝试从队列拿下一个
      while (!this.currentPacket || this.currentOffset >= this.currentPacket.length) {
        if (this.packetQueue.length === 0) {
          // 队列空了（无数据），剩余点数补静音 0
          this.currentPacket = null;
          this.currentOffset = 0;
          leftChannel[i] = 0;
          rightChannel[i] = 0;
          break;
        }
        this.currentPacket = this.packetQueue.shift();
        this.currentOffset = 0;
      }

      // 如果成功取到数据，提取并解交错 (L/R)
      if (this.currentPacket) {
        leftChannel[i] = this.currentPacket[this.currentOffset];
        rightChannel[i] = this.currentPacket[this.currentOffset + 1];
        this.currentOffset += 2; // 双声道步进 2
      }
    }

    return true;
  }
}

registerProcessor("pcm-player-processor", PCMPlayerProcessor);
