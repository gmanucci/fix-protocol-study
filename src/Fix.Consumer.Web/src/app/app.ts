import { Component } from '@angular/core';
import { MarketTickerComponent } from './market-ticker';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [MarketTickerComponent],
  template: '<app-market-ticker></app-market-ticker>',
  styleUrl: './app.scss'
})
export class App {}
