import { useRef, useEffect, memo } from 'react'

interface Props {
  history: number[]
  hue: number
  userId: string
}

function WaveformCanvas({ history, hue, userId }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    const cssW = canvas.clientWidth || 160
    const cssH = canvas.clientHeight || 36
    if (canvas.width !== cssW * dpr || canvas.height !== cssH * dpr) {
      canvas.width = cssW * dpr
      canvas.height = cssH * dpr
      ctx.scale(dpr, dpr)
    }

    ctx.clearRect(0, 0, cssW, cssH)

    const bars = 50
    const barW = cssW / bars
    const gap = 1.5

    // history[0] = newest sample (leftmost bar i=0)
    // taper: left (newest) is tallest, right (oldest) tapers to nothing
    for (let i = 0; i < bars; i++) {
      const amp = i < history.length ? history[i] : 0
      const taper = Math.pow(1 - i / bars, 0.75)
      const barH = Math.max(2, amp * cssH * 0.88 * taper)
      const x = i * barW
      const y = (cssH - barH) / 2
      const alpha = 0.25 + 0.75 * taper
      ctx.fillStyle = `oklch(72% 0.2 ${hue} / ${alpha})`
      ctx.beginPath()
      ctx.roundRect(x + gap / 2, y, barW - gap, barH, 2)
      ctx.fill()
    }
  }, [history, hue])

  return <canvas ref={canvasRef} className="waveform-canvas" id={`waveform-tip-${userId}`} />
}

export default memo(WaveformCanvas)
