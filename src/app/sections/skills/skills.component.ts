import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { ChipListComponent } from '../../shared/components/chip-list/chip-list.component';
import { SectionTitleComponent } from '../../shared/components/section-title/section-title.component';
import { RevealOnScrollDirective } from '../../shared/directives/reveal-on-scroll.directive';

@Component({
  selector: 'app-skills',
  standalone: true,
  imports: [SectionTitleComponent, ChipListComponent, RevealOnScrollDirective],
  templateUrl: './skills.component.html',
  styleUrl: './skills.component.scss'
})
export class SkillsComponent {
  readonly content = inject(I18nService).content;
}
