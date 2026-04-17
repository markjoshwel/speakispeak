import { useRef, useEffect, memo } from 'react'

interface Props {
  history: number[]
  hue: number
}

function WaveformCanvas({ history, hue }: Props) {
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

    for (let i = 0; i < bars; i++) {
      const histIdx = history.length - bars + i
      const amp = histIdx >= 0 ? history[histIdx] : 0
      const barH = Math.max(2, amp * cssH * 0.88)
      const x = i * barW
      const y = (cssH - barH) / 2
      const alpha = 0.25 + (i / bars) * 0.75
      ctx.fillStyle = `oklch(72% 0.2 ${hue} / ${alpha})`
      ctx.beginPath()
      ctx.roundRect(x + gap / 2, y, barW - gap, barH, 2)
      ctx.fill()
    }
  }, [history, hue])

  return <canvas ref={canvasRef} className="waveform-canvas" />
}

export default memo(WaveformCanvas)
