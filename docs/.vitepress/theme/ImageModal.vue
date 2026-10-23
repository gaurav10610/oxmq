<template>
  <Teleport to="body">
    <Transition name="modal-fade">
      <div
        v-if="isOpen"
        class="oxmq-media-modal"
        tabindex="-1"
        role="dialog"
        aria-modal="true"
        @keydown.esc="closeModal"
        @click.self="closeModal"
      >
        <!-- Modal Top Bar -->
        <header class="oxmq-modal-header" @click.stop>
          <div class="oxmq-modal-title">
            <span class="oxmq-badge">{{ isSvg ? 'Architecture Diagram' : 'Asset Preview' }}</span>
            <span class="oxmq-title-text" :title="title">{{ title || 'Visual Preview' }}</span>
          </div>

          <div class="oxmq-modal-toolbar">
            <span class="oxmq-zoom-level">{{ Math.round(scale * 100) }}%</span>

            <button
              type="button"
              class="oxmq-tool-btn"
              title="Zoom Out (-)"
              aria-label="Zoom Out"
              @click="zoomOut"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="11" cy="11" r="8"></circle>
                <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                <line x1="8" y1="11" x2="14" y2="11"></line>
              </svg>
            </button>

            <button
              type="button"
              class="oxmq-tool-btn"
              title="Reset Zoom (100%)"
              aria-label="Reset Zoom"
              @click="resetZoom"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"></path>
                <path d="M3 3v5h5"></path>
              </svg>
            </button>

            <button
              type="button"
              class="oxmq-tool-btn"
              title="Zoom In (+)"
              aria-label="Zoom In"
              @click="zoomIn"
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="11" cy="11" r="8"></circle>
                <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                <line x1="11" y1="8" x2="11" y2="14"></line>
                <line x1="8" y1="11" x2="14" y2="11"></line>
              </svg>
            </button>

            <button
              type="button"
              class="oxmq-close-btn"
              title="Close (Esc)"
              aria-label="Close Modal"
              @click="closeModal"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <line x1="18" y1="6" x2="6" y2="18"></line>
                <line x1="6" y1="6" x2="18" y2="18"></line>
              </svg>
            </button>
          </div>
        </header>

        <!-- Viewport Stage -->
        <div
          class="oxmq-modal-stage"
          :class="{ 'is-dragging': isDragging }"
          @wheel.prevent="handleWheel"
          @mousedown="startDrag"
          @mousemove="onDrag"
          @mouseup="endDrag"
          @mouseleave="endDrag"
          @click.self="closeModal"
        >
          <div
            class="oxmq-modal-content-wrapper"
            :style="transformStyle"
            @click.stop
          >
            <!-- SVG Diagram Rendering -->
            <div
              v-if="isSvg"
              class="oxmq-svg-container"
              v-html="svgContent"
            />

            <!-- Standard Image Rendering -->
            <img
              v-else
              :src="imageSrc"
              :alt="title"
              class="oxmq-image-view"
              draggable="false"
            />
          </div>
        </div>

        <!-- Footer Hint -->
        <footer class="oxmq-modal-footer" @click.stop>
          <span>Tip: Scroll to zoom &bull; Click &amp; drag to pan &bull; Press <kbd>Esc</kbd> to close</span>
        </footer>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'

const isOpen = ref(false)
const isSvg = ref(false)
const svgContent = ref('')
const imageSrc = ref('')
const title = ref('')

const scale = ref(1.0)
const translateX = ref(0)
const translateY = ref(0)
const isDragging = ref(false)
let startX = 0
let startY = 0

const transformStyle = computed(() => ({
  transform: `translate(${translateX.value}px, ${translateY.value}px) scale(${scale.value})`,
  cursor: scale.value > 1.05 ? (isDragging.value ? 'grabbing' : 'grab') : 'default'
}))

function zoomIn() {
  scale.value = Math.min(5.0, Number((scale.value + 0.25).toFixed(2)))
}

function zoomOut() {
  scale.value = Math.max(0.4, Number((scale.value - 0.25).toFixed(2)))
}

function resetZoom() {
  scale.value = 1.0
  translateX.value = 0
  translateY.value = 0
}

function handleWheel(e: WheelEvent) {
  if (e.deltaY < 0) {
    zoomIn()
  } else {
    zoomOut()
  }
}

function startDrag(e: MouseEvent) {
  if (scale.value <= 1.0) return
  isDragging.value = true
  startX = e.clientX - translateX.value
  startY = e.clientY - translateY.value
}

function onDrag(e: MouseEvent) {
  if (!isDragging.value) return
  translateX.value = e.clientX - startX
  translateY.value = e.clientY - startY
}

function endDrag() {
  isDragging.value = false
}

function findNearestHeading(el: HTMLElement): string {
  let prev = el.previousElementSibling as HTMLElement | null
  while (prev) {
    if (/^H[1-6]$/.test(prev.tagName)) {
      return prev.textContent?.trim().replace(/^#+\s*/, '') || ''
    }
    prev = prev.previousElementSibling as HTMLElement | null
  }
  if (el.parentElement) {
    let parentPrev = el.parentElement.previousElementSibling as HTMLElement | null
    while (parentPrev) {
      if (/^H[1-6]$/.test(parentPrev.tagName)) {
        return parentPrev.textContent?.trim().replace(/^#+\s*/, '') || ''
      }
      parentPrev = parentPrev.previousElementSibling as HTMLElement | null
    }
  }
  return ''
}

function openModalWithSvg(svg: SVGSVGElement, containerEl: HTMLElement) {
  isSvg.value = true
  imageSrc.value = ''
  
  const heading = findNearestHeading(containerEl)
  title.value = heading || 'Architecture Diagram'

  // Clone SVG element to preserve styles, defs, and markers
  const cloned = svg.cloneNode(true) as SVGSVGElement

  // 1. CRITICAL: Preserve the original ID so that scoped <style> rules `#${id}` match
  if (svg.id) {
    cloned.id = svg.id
  }

  // 2. Extract natural coordinate dimensions from viewBox or width/height attributes
  let nativeWidth = 960
  let nativeHeight = 600
  const viewBox = svg.getAttribute('viewBox') || cloned.getAttribute('viewBox')
  if (viewBox) {
    const parts = viewBox.trim().split(/[\s,]+/)
    if (parts.length === 4) {
      nativeWidth = parseFloat(parts[2]) || 960
      nativeHeight = parseFloat(parts[3]) || 600
    }
    cloned.setAttribute('viewBox', viewBox)
  } else {
    const w = parseFloat(svg.getAttribute('width') || cloned.getAttribute('width') || '')
    const h = parseFloat(svg.getAttribute('height') || cloned.getAttribute('height') || '')
    if (w && h) {
      nativeWidth = w
      nativeHeight = h
      cloned.setAttribute('viewBox', `0 0 ${w} ${h}`)
    }
  }

  // 3. Set explicit responsive vector dimensions on cloned SVG
  cloned.setAttribute('width', '100%')
  cloned.setAttribute('height', 'auto')
  cloned.style.display = 'block'
  cloned.style.width = '100%'
  cloned.style.maxWidth = `${Math.min(Math.max(nativeWidth, 600), 1400)}px`
  cloned.style.height = 'auto'
  cloned.style.maxHeight = '72vh'
  cloned.style.margin = '0 auto'

  svgContent.value = cloned.outerHTML
  resetZoom()
  isOpen.value = true
  if (typeof document !== 'undefined') {
    document.body.style.overflow = 'hidden'
  }
}

function openModalWithImage(img: HTMLImageElement) {
  isSvg.value = false
  svgContent.value = ''
  imageSrc.value = img.currentSrc || img.src

  const heading = findNearestHeading(img)
  title.value = img.alt || img.getAttribute('title') || heading || 'Image Preview'

  resetZoom()
  isOpen.value = true
  if (typeof document !== 'undefined') {
    document.body.style.overflow = 'hidden'
  }
}

function closeModal() {
  isOpen.value = false
  svgContent.value = ''
  imageSrc.value = ''
  resetZoom()
  if (typeof document !== 'undefined') {
    document.body.style.overflow = ''
  }
}

function onKeydown(e: KeyboardEvent) {
  if (!isOpen.value) return
  if (e.key === 'Escape') closeModal()
  if (e.key === '+' || e.key === '=') zoomIn()
  if (e.key === '-' || e.key === '_') zoomOut()
  if (e.key === '0') resetZoom()
}

function handleGlobalClick(e: MouseEvent) {
  const target = e.target as HTMLElement | null
  if (!target) return

  // Safeguard against non-Element nodes (e.g. text nodes)
  const el = (target instanceof Element ? target : target.parentElement) as HTMLElement | null
  if (!el) return

  // 1. Mermaid diagram check
  const mermaidContainer = el.closest('.mermaid') as HTMLElement | null
  if (mermaidContainer) {
    const svg = (el.tagName.toLowerCase() === 'svg' ? el : el.closest('svg')) || mermaidContainer.querySelector('svg')
    if (svg) {
      e.preventDefault()
      e.stopPropagation()
      openModalWithSvg(svg as SVGSVGElement, mermaidContainer)
      return
    }
  }

  // 2. Image check inside documentation content
  const img = el.closest('.vp-doc img:not(.logo):not(.vp-icon)') as HTMLImageElement | null
  if (img) {
    e.preventDefault()
    e.stopPropagation()
    openModalWithImage(img)
    return
  }
}

onMounted(() => {
  if (typeof window !== 'undefined') {
    document.addEventListener('click', handleGlobalClick, true)
    window.addEventListener('keydown', onKeydown)
  }
})

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    document.removeEventListener('click', handleGlobalClick, true)
    window.removeEventListener('keydown', onKeydown)
    document.body.style.overflow = ''
  }
})
</script>

<style scoped>
.oxmq-media-modal {
  position: fixed;
  inset: 0;
  z-index: 99999;
  display: flex;
  flex-direction: column;
  background-color: rgba(10, 12, 16, 0.90);
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
  user-select: none;
}

/* Header */
.oxmq-modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 24px;
  background: rgba(22, 27, 34, 0.85);
  border-bottom: 1px solid rgba(255, 255, 255, 0.12);
  color: #f0f6fc;
  z-index: 10;
}

.oxmq-modal-title {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 14px;
  font-weight: 500;
  max-width: 60%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.oxmq-badge {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  background: var(--vp-c-brand-1, #3b82f6);
  color: #ffffff;
  padding: 3px 10px;
  border-radius: 6px;
  flex-shrink: 0;
}

.oxmq-title-text {
  color: #e6edf3;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* Toolbar */
.oxmq-modal-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.oxmq-zoom-level {
  font-size: 13px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  color: #8b949e;
  min-width: 46px;
  text-align: right;
  margin-right: 4px;
}

.oxmq-tool-btn,
.oxmq-close-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid rgba(255, 255, 255, 0.14);
  color: #e6edf3;
  cursor: pointer;
  transition: all 0.15s ease;
}

.oxmq-tool-btn:hover {
  background: rgba(255, 255, 255, 0.20);
  color: #ffffff;
}

.oxmq-close-btn {
  background: rgba(239, 68, 68, 0.25);
  border-color: rgba(239, 68, 68, 0.4);
  color: #fca5a5;
  margin-left: 6px;
}

.oxmq-close-btn:hover {
  background: rgba(239, 68, 68, 0.9);
  color: #ffffff;
}

/* Viewport Stage */
.oxmq-modal-stage {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  position: relative;
  padding: 32px;
}

.oxmq-modal-content-wrapper {
  max-width: 92vw;
  max-height: 82vh;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: transform 0.1s ease-out;
  transform-origin: center center;
}

/* SVG Rendering Container */
.oxmq-svg-container {
  background: var(--vp-c-bg-elv, #ffffff);
  border: 1px solid var(--vp-c-divider, rgba(255, 255, 255, 0.15));
  padding: 24px 32px;
  border-radius: 12px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.55);
  width: min(92vw, 1280px);
  max-width: 92vw;
  max-height: 80vh;
  overflow: auto;
  box-sizing: border-box;
}

.oxmq-svg-container :deep(svg) {
  display: block;
  margin: 0 auto;
}

/* Image Rendering */
.oxmq-image-view {
  max-width: 88vw;
  max-height: 80vh;
  object-fit: contain;
  border-radius: 8px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.6);
}

/* Footer */
.oxmq-modal-footer {
  text-align: center;
  padding: 10px 16px;
  font-size: 12px;
  color: #8b949e;
  background: rgba(22, 27, 34, 0.7);
  border-top: 1px solid rgba(255, 255, 255, 0.08);
}

.oxmq-modal-footer kbd {
  background: rgba(255, 255, 255, 0.16);
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 11px;
  color: #f0f6fc;
}

/* Transitions */
.modal-fade-enter-active,
.modal-fade-leave-active {
  transition: opacity 0.2s ease;
}

.modal-fade-enter-from,
.modal-fade-leave-to {
  opacity: 0;
}
</style>
