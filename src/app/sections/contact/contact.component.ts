import { Component, inject, signal } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { SectionTitleComponent } from '../../shared/components/section-title/section-title.component';
import { RevealOnScrollDirective } from '../../shared/directives/reveal-on-scroll.directive';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-contact',
  standalone: true,
  imports: [SectionTitleComponent, RevealOnScrollDirective, RichTextPipe],
  templateUrl: './contact.component.html',
  styleUrl: './contact.component.scss'
})
export class ContactComponent {
  readonly content = inject(I18nService).content;
  readonly captchaVisible = signal(false);
  readonly emailUnlocked = signal(false);
  readonly captchaA = signal(0);
  readonly captchaB = signal(0);
  readonly captchaAnswer = signal('');
  readonly captchaError = signal(false);

  requestEmailAccess(): void {
    this.captchaVisible.set(true);
    this.emailUnlocked.set(false);
    this.captchaAnswer.set('');
    this.captchaError.set(false);
    this.generateCaptcha();
  }

  updateCaptchaAnswer(value: string): void {
    this.captchaAnswer.set(value);
    this.captchaError.set(false);
  }

  validateCaptcha(): void {
    const expected = this.captchaA() + this.captchaB();
    const provided = Number(this.captchaAnswer());

    if (provided === expected) {
      this.emailUnlocked.set(true);
      this.captchaVisible.set(false);
      this.captchaError.set(false);
      return;
    }

    this.captchaError.set(true);
    this.generateCaptcha();
  }

  private generateCaptcha(): void {
    this.captchaA.set(Math.floor(Math.random() * 8) + 2);
    this.captchaB.set(Math.floor(Math.random() * 8) + 2);
  }
}
