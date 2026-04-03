import { Component, input } from '@angular/core';
import { RichTextPipe } from '../../pipes/rich-text.pipe';

@Component({
  selector: 'app-chip-list',
  standalone: true,
  imports: [RichTextPipe],
  templateUrl: './chip-list.component.html',
  styleUrl: './chip-list.component.scss'
})
export class ChipListComponent {
  readonly items = input.required<string[]>();
}
