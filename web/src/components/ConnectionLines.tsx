import { useRef, useEffect } from 'react'
import type { ActiveRoute } from '../types'
import { userHue } from '../utils'

interface Props {
  routes: ActiveRoute[]
}

function pt(id: string, side: 'right' | 'center'): { x: number; y: number } | null {
  const el = document.getElementById(id)
  if (!el) return null
  const r = el.getBoundingClientRect()
  return side === 'right'
    ? { x: r.right, y: r.top + r.height / 2 }
    : { x: r.left + r.width / 2, y: r.top + r.height / 2 }
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

        const from = pt(`user-card-${route.user_id}`, 'right')
        const to = pt(`worker-${route.worker_idx}`, 'center')
        if (!from || !to) continue

        const opacity = Math.max(0, 1 - age / 2200)
        const hue = userHue(route.user_id)
        const midX = from.x + (to.x - from.x) * 0.5

        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path')
        path.setAttribute(
          'd',
          `M ${from.x} ${from.y} C ${midX} ${from.y} ${midX} ${to.y} ${to.x} ${to.y}`,
        )
        path.setAttribute('stroke', `oklch(72% 0.2 ${hue} / ${opacity})`)
        path.setAttribute('stroke-width', '2')
        path.setAttribute('fill', 'none')
        path.setAttribute('stroke-linecap', 'round')
        fragment.appendChild(path)
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
