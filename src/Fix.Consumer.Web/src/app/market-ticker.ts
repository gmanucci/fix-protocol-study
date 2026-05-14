import { Component, OnDestroy, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription as RxSubscription } from 'rxjs';
import { SignalrService } from './signalr.service';
import { MarketTick, Subscription, Transport } from './market.models';

interface RowState {
  key: string;
  symbol: string;
  transport: Transport;
  lastPrice?: number;
  lastQty?: number;
  lastEntry?: string;
  lastUpdateTicks?: number;
  count: number;
}

@Component({
  selector: 'app-market-ticker',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './market-ticker.html',
  styleUrl: './market-ticker.scss',
})
export class MarketTickerComponent implements OnInit, OnDestroy {
  readonly availableSymbols = ['EURUSD', 'GBPUSD', 'USDJPY', 'AAPL', 'MSFT'];
  readonly transports: Transport[] = ['TCP', 'UDP'];

  symbol = 'EURUSD';
  transport: Transport = 'TCP';

  rows = signal<RowState[]>([]);
  status = signal<string>('disconnected');

  private sub?: RxSubscription;
  private rowMap = new Map<string, RowState>();

  constructor(public hub: SignalrService) {}

  async ngOnInit(): Promise<void> {
    this.sub = this.hub.tick$.subscribe((t: MarketTick) => this.onTick(t));
    try {
      await this.hub.ensureConnected();
      this.status.set('connected');
    } catch (e) {
      this.status.set('error: ' + (e as Error).message);
    }
  }

  ngOnDestroy(): void {
    this.sub?.unsubscribe();
  }

  async subscribe(): Promise<void> {
    const key = this.keyFor({ symbol: this.symbol, transport: this.transport });
    if (this.rowMap.has(key)) return;
    const row: RowState = { key, symbol: this.symbol, transport: this.transport, count: 0 };
    this.rowMap.set(key, row);
    this.rows.set([...this.rowMap.values()]);
    await this.hub.subscribe(this.symbol, this.transport);
  }

  async unsubscribe(row: RowState): Promise<void> {
    await this.hub.unsubscribe(row.symbol, row.transport);
    this.rowMap.delete(row.key);
    this.rows.set([...this.rowMap.values()]);
  }

  private onTick(t: MarketTick): void {
    const key = this.keyFor({ symbol: t.symbol, transport: t.transport });
    const row = this.rowMap.get(key);
    if (!row) return;
    row.lastPrice = t.price;
    row.lastQty = t.quantity;
    row.lastEntry = t.entryType;
    row.lastUpdateTicks = t.timestampTicks;
    row.count++;
    // Trigger change detection by replacing the array reference.
    this.rows.set([...this.rowMap.values()]);
  }

  private keyFor(s: Subscription): string {
    return `${s.symbol}|${s.transport}`;
  }
}
