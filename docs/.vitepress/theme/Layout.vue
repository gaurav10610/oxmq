<template>
  <DefaultTheme.Layout>
    <template #layout-bottom>
      <ImageModal />
    </template>
  </DefaultTheme.Layout>
</template>

<script setup lang="ts">
import DefaultTheme from 'vitepress/theme'
import ImageModal from './ImageModal.vue'
</script>
