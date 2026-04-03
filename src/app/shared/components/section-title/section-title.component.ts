import { Component, input } from '@angular/core';
import { RichTextPipe } from '../../pipes/rich-text.pipe';

@Component({
  selector: 'app-section-title',
  standalone: true,
  imports: [RichTextPipe],
  templateUrl: './section-title.component.html',
  styleUrl: './section-title.component.scss'
})
export class SectionTitleComponent {
  readonly title = input.required<string>();
  readonly id = input<string>();
}
