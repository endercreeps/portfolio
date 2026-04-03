import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { ThemeService } from '../../core/services/theme.service';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-header',
  standalone: true,
  imports: [RichTextPipe],
  templateUrl: './header.component.html',
  styleUrl: './header.component.scss'
})
export class HeaderComponent {
  private readonly i18n = inject(I18nService);
  private readonly theme = inject(ThemeService);

  readonly content = this.i18n.content;
  readonly language = this.i18n.language;
  readonly currentTheme = this.theme.theme;

  scrollToSection(sectionId: string): void {
    const section = document.getElementById(sectionId);
    section?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  switchLanguage(language: 'fr' | 'en'): void {
    this.i18n.setLanguage(language);
  }

  toggleTheme(): void {
    this.theme.toggleTheme();
  }
}
