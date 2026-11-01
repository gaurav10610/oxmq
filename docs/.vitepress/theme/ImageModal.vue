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
            <span class="oxmq-badge" :class="{ 'is-video': isVideo, 'is-svg': isSvg }">
              {{ isVideo ? '🎬 Motion Diagram' : (isSvg ? 'Architecture Diagram' : 'Asset Preview') }}
            </span>
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
              class="oxmq-tool-btn"
              :title="isFullscreen ? 'Exit Fullscreen (F)' : 'Toggle Fullscreen (F)'"
              aria-label="Toggle Fullscreen"
              @click="toggleFullscreen"
            >
              <svg v-if="!isFullscreen" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"></path>
              </svg>
              <svg v-else width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M4 14h6m0 0v6m0-6L3 21m17-7h-6m0 0v6m0-6l7 7M4 10h6m0 0V4m0 6L3 3m17 7h-6m0 0V4m0 6l7-7"></path>
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

            <!-- Motion Video Rendering -->
            <div
              v-else-if="isVideo"
              class="oxmq-video-container"
            >
              <video
                ref="modalVideoEl"
                :src="videoSrc"
                autoplay
                loop
                controls
                playsinline
                class="oxmq-video-view"
              />
            </div>

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
          <span>Tip: Scroll to zoom ({{ Math.round(scale * 100) }}%) &bull; Click &amp; drag to pan &bull; Press <kbd>F</kbd> for fullscreen &bull; Press <kbd>Esc</kbd> to close</span>
        </footer>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'

const isOpen = ref(false)
const isSvg = ref(false)
const isVideo = ref(false)
const svgContent = ref('')
const imageSrc = ref('')
const videoSrc = ref('')
const title = ref('')
const isFullscreen = ref(false)
const modalVideoEl = ref<HTMLVideoElement | null>(null)

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

function toggleFullscreen() {
  if (typeof document === 'undefined') return
  if (!document.fullscreenElement) {
    document.documentElement.requestFullscreen?.().then(() => {
      isFullscreen.value = true
    }).catch(() => {})
  } else {
    document.exitFullscreen?.().then(() => {
      isFullscreen.value = false
    }).catch(() => {})
  }
}

function handleWheel(e: WheelEvent) {
  if (e.deltaY < 0) {
    zoomIn()
  } else {
    zoomOut()
  }
}

function startDrag(e: MouseEvent) {
  // Allow dragging if zoomed in or if target is not interactive controls
  if (scale.value <= 1.0) return
  const target = e.target as HTMLElement
  if (target && target.tagName.toLowerCase() === 'video' && target.hasAttribute('controls')) {
    // If clicking video controls, don't drag
    const rect = target.getBoundingClientRect()
    if (e.clientY > rect.bottom - 50) return
  }
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
  isVideo.value = false
  imageSrc.value = ''
  videoSrc.value = ''
  
  const heading = findNearestHeading(containerEl)
  title.value = heading || 'Architecture Diagram'

  const cloned = svg.cloneNode(true) as SVGSVGElement

  if (svg.id) {
    cloned.id = svg.id
  }

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

function openModalWithVideo(video: HTMLVideoElement) {
  isVideo.value = true
  isSvg.value = false
  svgContent.value = ''
  imageSrc.value = ''

  const source = video.querySelector('source')
  videoSrc.value = source?.src || video.currentSrc || video.src

  const heading = findNearestHeading(video)
  title.value = video.getAttribute('aria-label') || video.getAttribute('title') || heading || 'Motion Diagram'

  resetZoom()
  isOpen.value = true
  if (typeof document !== 'undefined') {
    document.body.style.overflow = 'hidden'
  }
}

function openModalWithImage(img: HTMLImageElement) {
  isSvg.value = false
  isVideo.value = false
  svgContent.value = ''
  videoSrc.value = ''
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
  isSvg.value = false
  isVideo.value = false
  svgContent.value = ''
  imageSrc.value = ''
  videoSrc.value = ''
  resetZoom()
  if (typeof document !== 'undefined') {
    document.body.style.overflow = ''
    if (document.fullscreenElement) {
      document.exitFullscreen?.().catch(() => {})
      isFullscreen.value = false
    }
  }
}

function onKeydown(e: KeyboardEvent) {
  if (!isOpen.value) return
  if (e.key === 'Escape') closeModal()
  if (e.key === '+' || e.key === '=') zoomIn()
  if (e.key === '-' || e.key === '_') zoomOut()
  if (e.key === '0') resetZoom()
  if (e.key === 'f' || e.key === 'F') toggleFullscreen()
}

function handleGlobalClick(e: MouseEvent) {
  const target = e.target as HTMLElement | null
  if (!target) return

  const el = (target instanceof Element ? target : target.parentElement) as HTMLElement | null
  if (!el) return

  // 1. Check if clicked inside modal itself
  if (el.closest('.oxmq-media-modal')) return

  // 2. Mermaid diagram check
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

  // 3. Motion Video check inside documentation content
  const video = el.closest('.vp-doc video, .oxmq-motion-wrapper video') as HTMLVideoElement | null
  if (video) {
    e.preventDefault()
    e.stopPropagation()
    openModalWithVideo(video)
    return
  }

  // 4. Motion wrapper container check (even if clicked on wrapper padding/overlay)
  const motionWrapper = el.closest('.oxmq-motion-wrapper') as HTMLElement | null
  if (motionWrapper) {
    const innerVideo = motionWrapper.querySelector('video')
    if (innerVideo) {
      e.preventDefault()
      e.stopPropagation()
      openModalWithVideo(innerVideo)
      return
    }
  }

  // 5. Image check inside documentation content
  const img = el.closest('.vp-doc img:not(.logo):not(.vp-icon)') as HTMLImageElement | null
  if (img) {
    // If inside a motion video container that has a video element, prefer the video
    const parentContainer = img.closest('div')
    const siblingVideo = parentContainer?.querySelector('video')
    if (siblingVideo) {
      e.preventDefault()
      e.stopPropagation()
      openModalWithVideo(siblingVideo)
      return
    }

    e.preventDefault()
    e.stopPropagation()
    openModalWithImage(img)
    return
  }
}

function onFullscreenChange() {
  if (typeof document !== 'undefined') {
    isFullscreen.value = !!document.fullscreenElement
  }
}

onMounted(() => {
  if (typeof window !== 'undefined') {
    document.addEventListener('click', handleGlobalClick, true)
    window.addEventListener('keydown', onKeydown)
    document.addEventListener('fullscreenchange', onFullscreenChange)
  }
})

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    document.removeEventListener('click', handleGlobalClick, true)
    window.removeEventListener('keydown', onKeydown)
    document.removeEventListener('fullscreenchange', onFullscreenChange)
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
  background-color: rgba(10, 12, 16, 0.94);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  user-select: none;
}

/* Header */
.oxmq-modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 24px;
  background: rgba(22, 27, 34, 0.85);
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
  z-index: 10;
}

.oxmq-modal-title {
  display: flex;
  align-items: center;
  gap: 12px;
  overflow: hidden;
  max-width: 60%;
}

.oxmq-badge {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding: 3px 10px;
  border-radius: 20px;
  background: rgba(56, 189, 248, 0.15);
  color: #38bdf8;
  border: 1px solid rgba(56, 189, 248, 0.35);
  white-space: nowrap;
}

.oxmq-badge.is-video {
  background: rgba(245, 158, 11, 0.18);
  color: #fbbf24;
  border-color: rgba(245, 158, 11, 0.4);
}

.oxmq-badge.is-svg {
  background: rgba(168, 85, 247, 0.15);
  color: #c084fc;
  border-color: rgba(168, 85, 247, 0.35);
}

.oxmq-title-text {
  font-size: 15px;
  font-weight: 600;
  color: #f0f6fc;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* Toolbar */
.oxmq-modal-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.oxmq-zoom-level {
  font-family: var(--vp-font-family-mono, monospace);
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
  max-width: 95vw;
  max-height: 84vh;
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

/* Video Rendering */
.oxmq-video-container {
  display: flex;
  align-items: center;
  justify-content: center;
  max-width: 94vw;
  max-height: 82vh;
}

.oxmq-video-view {
  max-width: 92vw;
  max-height: 80vh;
  width: auto;
  height: auto;
  border-radius: 12px;
  box-shadow: 0 25px 60px rgba(0, 0, 0, 0.7);
  border: 1px solid rgba(56, 189, 248, 0.35);
  display: block;
  outline: none;
}

/* Image Rendering */
.oxmq-image-view {
  max-width: 90vw;
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
