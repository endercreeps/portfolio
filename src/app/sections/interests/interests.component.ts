import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { SectionTitleComponent } from '../../shared/components/section-title/section-title.component';
import { RevealOnScrollDirective } from '../../shared/directives/reveal-on-scroll.directive';

@Component({
  selector: 'app-interests',
  standalone: true,
  imports: [SectionTitleComponent, RevealOnScrollDirective],
  templateUrl: './interests.component.html',
  styleUrl: './interests.component.scss'
})
export class InterestsComponent {
  readonly content = inject(I18nService).content;
}

