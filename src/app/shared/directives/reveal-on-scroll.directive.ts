import { Directive, ElementRef, OnDestroy, OnInit, Renderer2, inject } from '@angular/core';

@Directive({
  selector: '[appRevealOnScroll]',
  standalone: true
})
export class RevealOnScrollDirective implements OnInit, OnDestroy {
  private readonly element = inject(ElementRef<HTMLElement>);
  private readonly renderer = inject(Renderer2);
  private observer?: IntersectionObserver;

  ngOnInit(): void {
    const hasMatchMedia = typeof window !== 'undefined' && typeof window.matchMedia === 'function';
    if (hasMatchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      this.renderer.addClass(this.element.nativeElement, 'is-visible');
      return;
    }

    if (typeof IntersectionObserver === 'undefined') {
      this.renderer.addClass(this.element.nativeElement, 'is-visible');
      return;
    }

    this.observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            this.renderer.addClass(entry.target, 'is-visible');
            this.observer?.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.16 }
    );

    this.observer.observe(this.element.nativeElement);
  }

  ngOnDestroy(): void {
    this.observer?.disconnect();
  }
}
