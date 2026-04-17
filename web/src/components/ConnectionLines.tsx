import { useRef, useEffect } from 'react'
import type { ActiveRoute } from '../types'
import { userHue } from '../utils'

interface Props {
  routes: ActiveRoute[]
}

function pt(id: string, side: 'right' | 'left' | 'center'): { x: number; y: number } | null {
  const el = document.getElementById(id)
  if (!el) return null
  const r = el.getBoundingClientRect()
  if (side === 'right')  return { x: r.right,  y: r.top + r.height / 2 }
  if (side === 'left')   return { x: r.left,   y: r.top + r.height / 2 }
  return { x: r.left + r.width / 2, y: r.top + r.height / 2 }
}

export default function ConnectionLines({ routes }: Props) {
  const svgRef = useRef<SVGSVGElement>(null)
  const routesRef = useRef(routes)
  const rafRef = useRef<number>(0)

  routesRef.current = routes

  useEffect(() => {
    const draw = () => {
      const svg = svgRef.current
      if (!svg) {
        rafRef.current = requestAnimationFrame(draw)
        return
      }

      const now = Date.now()
      const fragment = document.createDocumentFragment()

      for (const route of routesRef.current) {
        const age = now - route.at
        if (age > 2200) continue

        const waveformTip = pt(`waveform-tip-${route.user_id}`, 'right')
        const workerCenter = pt(`worker-${route.worker_idx}`, 'center')
        const txLeft = pt(`tx-${route.user_id}`, 'left')
        if (!waveformTip || !workerCenter) continue

        const opacity = Math.max(0, 1 - age / 2200)
        const hue = userHue(route.user_id)
        const stroke = `oklch(72% 0.2 ${hue} / ${opacity})`

        // Left line: waveform tip → worker circle
        const left = document.createElementNS('http://www.w3.org/2000/svg', 'path')
        left.setAttribute('d', `M ${waveformTip.x} ${waveformTip.y} L ${workerCenter.x} ${workerCenter.y}`)
        left.setAttribute('stroke', stroke)
        left.setAttribute('stroke-width', '1.5')
        left.setAttribute('fill', 'none')
        fragment.appendChild(left)

        // Right line: worker circle → transcription row left edge
        if (txLeft) {
          const right = document.createElementNS('http://www.w3.org/2000/svg', 'path')
          right.setAttribute('d', `M ${workerCenter.x} ${workerCenter.y} L ${txLeft.x} ${txLeft.y}`)
          right.setAttribute('stroke', stroke)
          right.setAttribute('stroke-width', '1.5')
          right.setAttribute('fill', 'none')
          fragment.appendChild(right)
        }
      }

      svg.replaceChildren(fragment)
      rafRef.current = requestAnimationFrame(draw)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => cancelAnimationFrame(rafRef.current)
  }, [])

  return (
    <svg
      ref={svgRef}
      style={{
        position: 'fixed',
        inset: 0,
        width: '100vw',
        height: '100vh',
        pointerEvents: 'none',
        zIndex: 10,
        overflow: 'visible',
      }}
    />
  )
}
