import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { SectionTitleComponent } from '../../shared/components/section-title/section-title.component';
import { RevealOnScrollDirective } from '../../shared/directives/reveal-on-scroll.directive';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-school',
  standalone: true,
  imports: [SectionTitleComponent, RevealOnScrollDirective, RichTextPipe],
  templateUrl: './school.component.html',
  styleUrl: './school.component.scss'
})
export class SchoolComponent {
  readonly content = inject(I18nService).content;
}
