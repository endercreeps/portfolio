import { Injectable, computed, signal } from '@angular/core';
import { CONTENT_EN } from '../../data/content.en';
import { CONTENT_FR } from '../../data/content.fr';
import { SITE_CONFIG } from '../../data/site.config';
import { Language, PortfolioContent } from '../../models/content.models';

const STORAGE_KEY = 'portfolio-language';

@Injectable({ providedIn: 'root' })
export class I18nService {
  private readonly currentLanguage = signal<Language>(this.resolveInitialLanguage());

  readonly language = computed(() => this.currentLanguage());
  readonly content = computed<PortfolioContent>(() =>
    this.currentLanguage() === 'fr' ? CONTENT_FR : CONTENT_EN
  );

  setLanguage(language: Language): void {
    this.currentLanguage.set(language);
    localStorage.setItem(STORAGE_KEY, language);
    document.documentElement.lang = language;
  }

  toggleLanguage(): void {
    this.setLanguage(this.currentLanguage() === 'fr' ? 'en' : 'fr');
  }

  private resolveInitialLanguage(): Language {
    const saved = localStorage.getItem(STORAGE_KEY) as Language | null;
    const language = saved === 'fr' || saved === 'en' ? saved : SITE_CONFIG.defaultLanguage;
    document.documentElement.lang = language;
    return language;
  }
}
