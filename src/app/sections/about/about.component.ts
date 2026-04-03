import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { SectionTitleComponent } from '../../shared/components/section-title/section-title.component';
import { RevealOnScrollDirective } from '../../shared/directives/reveal-on-scroll.directive';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-about',
  standalone: true,
  imports: [SectionTitleComponent, RevealOnScrollDirective, RichTextPipe],
  templateUrl: './about.component.html',
  styleUrl: './about.component.scss'
})
export class AboutComponent {
  readonly content = inject(I18nService).content;
}
