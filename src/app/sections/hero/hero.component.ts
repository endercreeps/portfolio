import {
  AfterViewInit,
  Component,
  ElementRef,
  OnDestroy,
  ViewChild,
  computed,
  inject,
  signal
} from '@angular/core';
import * as THREE from 'three';
import { SITE_CONFIG } from '../../data/site.config';
import { I18nService } from '../../core/services/i18n.service';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-hero',
  standalone: true,
  imports: [RichTextPipe],
  templateUrl: './hero.component.html',
  styleUrl: './hero.component.scss'
})
export class HeroComponent implements AfterViewInit, OnDestroy {
  @ViewChild('heroCanvas') private readonly heroCanvas?: ElementRef<HTMLCanvasElement>;

  private readonly i18n = inject(I18nService);
  readonly content = this.i18n.content;
  readonly cvPath = SITE_CONFIG.cvPath;
  readonly reducedMotion = signal(false);
  readonly supportsWebgl = signal(true);
  readonly showCanvas = computed(() => !this.reducedMotion() && this.supportsWebgl());

  private renderer?: THREE.WebGLRenderer;
  private scene?: THREE.Scene;
  private camera?: THREE.PerspectiveCamera;
  private points?: THREE.Points;
  private frameId?: number;

  ngAfterViewInit(): void {
    const hasMatchMedia = typeof window !== 'undefined' && typeof window.matchMedia === 'function';
    this.reducedMotion.set(
      hasMatchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)').matches : false
    );
    if (this.reducedMotion()) {
      return;
    }

    const canvas = this.heroCanvas?.nativeElement;
    if (!canvas) {
      return;
    }

    try {
      this.initScene(canvas);
      this.animate();
      window.addEventListener('resize', this.onResize);
    } catch {
      this.supportsWebgl.set(false);
      this.destroyScene();
    }
  }

  ngOnDestroy(): void {
    window.removeEventListener('resize', this.onResize);
    this.destroyScene();
  }

  private initScene(canvas: HTMLCanvasElement): void {
    const { width, height } = canvas.getBoundingClientRect();

    this.scene = new THREE.Scene();
    this.camera = new THREE.PerspectiveCamera(50, width / Math.max(height, 1), 0.1, 100);
    this.camera.position.set(0, 0, 6);

    this.renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true, powerPreference: 'high-performance' });
    this.renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.8));
    this.renderer.setSize(width, height, false);

    const geometry = new THREE.BufferGeometry();
    const count = width < 768 ? 420 : 720;
    const positions = new Float32Array(count * 3);
    const colors = new Float32Array(count * 3);

    const palette = [
      new THREE.Color(0x4fd1c5),
      new THREE.Color(0x60a5fa),
      new THREE.Color(0xf472b6),
      new THREE.Color(0xf59e0b),
      new THREE.Color(0x34d399)
    ];

    for (let i = 0; i < count; i += 1) {
      const idx = i * 3;
      positions[idx] = (Math.random() - 0.5) * 10;
      positions[idx + 1] = (Math.random() - 0.5) * 6;
      positions[idx + 2] = (Math.random() - 0.5) * 4;

      const base = palette[Math.floor(Math.random() * palette.length)].clone();
      base.offsetHSL((Math.random() - 0.5) * 0.045, 0.06, 0.04);
      colors[idx] = base.r;
      colors[idx + 1] = base.g;
      colors[idx + 2] = base.b;
    }

    geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    geometry.setAttribute('color', new THREE.BufferAttribute(colors, 3));

    const material = new THREE.PointsMaterial({
      vertexColors: true,
      size: width < 768 ? 0.05 : 0.038,
      transparent: true,
      opacity: 0.72,
      depthWrite: false,
      blending: THREE.NormalBlending
    });

    this.points = new THREE.Points(geometry, material);
    this.scene.add(this.points);

    const ambient = new THREE.AmbientLight(0xffffff, 0.42);
    this.scene.add(ambient);
  }

  private animate = (): void => {
    if (!this.renderer || !this.scene || !this.camera || !this.points) {
      return;
    }

    this.points.rotation.y += 0.00045;
    this.points.rotation.x += 0.00015;

    this.renderer.render(this.scene, this.camera);
    this.frameId = window.requestAnimationFrame(this.animate);
  };

  private onResize = (): void => {
    if (!this.renderer || !this.camera || !this.heroCanvas) {
      return;
    }

    const canvas = this.heroCanvas.nativeElement;
    const { width, height } = canvas.getBoundingClientRect();
    this.renderer.setSize(width, height, false);
    this.camera.aspect = width / Math.max(height, 1);
    this.camera.updateProjectionMatrix();
  };

  private destroyScene(): void {
    if (this.frameId) {
      window.cancelAnimationFrame(this.frameId);
    }

    this.points?.geometry.dispose();
    if (this.points?.material instanceof THREE.Material) {
      this.points.material.dispose();
    }

    this.renderer?.dispose();

    this.points = undefined;
    this.renderer = undefined;
    this.scene = undefined;
    this.camera = undefined;
    this.frameId = undefined;
  }
}
