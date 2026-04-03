import { Component, inject } from '@angular/core';
import { I18nService } from '../../core/services/i18n.service';
import { RichTextPipe } from '../../shared/pipes/rich-text.pipe';

@Component({
  selector: 'app-footer',
  standalone: true,
  imports: [RichTextPipe],
  templateUrl: './footer.component.html',
  styleUrl: './footer.component.scss'
})
export class FooterComponent {
  readonly content = inject(I18nService).content;
  readonly year = new Date().getFullYear();
}
