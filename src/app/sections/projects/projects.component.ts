import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { ChipListComponent } from '../../shared/components/chip-list/chip-list.component';
import { SectionTitleComponent } from '../../shared/components/section-title/section-title.component';
import { RevealOnScrollDirective } from '../../shared/directives/reveal-on-scroll.directive';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-projects',
  standalone: true,
  imports: [SectionTitleComponent, ChipListComponent, RevealOnScrollDirective, RichTextPipe],
  templateUrl: './projects.component.html',
  styleUrl: './projects.component.scss'
})
export class ProjectsComponent {
  readonly content = inject(I18nService).content;
}
