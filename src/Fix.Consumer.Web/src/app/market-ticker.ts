import { Component, OnDestroy, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Subscription as RxSubscription } from 'rxjs';
import { SignalrService } from './signalr.service';
import { FixEvent, MarketTick, Subscription, Transport } from './market.models';

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

interface SessionLogRow {
  ts: number;
  kind: string;
  symbol: string | null;
  source: string;
  detail: string;
}

const DEFAULT_KINDS = ['MarketDataIncrementalRefresh'];

@Component({
  selector: 'app-market-ticker',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './market-ticker.html',
  styleUrl: './market-ticker.scss',
})
export class MarketTickerComponent implements OnInit, OnDestroy {
  readonly availableSymbols = ['EURUSD', 'GBPUSD', 'USDJPY', 'AAPL', 'MSFT'];
  readonly transports: Transport[] = ['Tcp', 'Udp', 'QuickFix'];

  symbol = 'EURUSD';
  transport: Transport = 'Tcp';

  /** Populated dynamically from the hub via ListEventKinds(). */
  availableKinds = signal<string[]>([]);
  selectedKinds: Record<string, boolean> = { MarketDataIncrementalRefresh: true };

  rows = signal<RowState[]>([]);
  sessionLog = signal<SessionLogRow[]>([]);
  status = signal<string>('disconnected');

  private tickSub?: RxSubscription;
  private eventSub?: RxSubscription;
  private rowMap = new Map<string, RowState>();

  constructor(public hub: SignalrService) {}

  async ngOnInit(): Promise<void> {
    this.tickSub = this.hub.tick$.subscribe((t: MarketTick) => this.onTick(t));
    this.eventSub = this.hub.event$.subscribe((e: FixEvent) => this.onEvent(e));
    try {
      await this.hub.ensureConnected();
      this.status.set('connected');
      const kinds = await this.hub.listEventKinds();
      this.availableKinds.set(kinds);
      // Default selection: only the original behaviour.
      for (const k of kinds) {
        if (!(k in this.selectedKinds)) this.selectedKinds[k] = DEFAULT_KINDS.includes(k);
      }
    } catch (e) {
      this.status.set('error: ' + (e as Error).message);
    }
  }

  ngOnDestroy(): void {
    this.tickSub?.unsubscribe();
    this.eventSub?.unsubscribe();
  }

  selectedKindNames(): string[] {
    return Object.entries(this.selectedKinds).filter(([, v]) => v).map(([k]) => k);
  }

  async subscribe(): Promise<void> {
    const key = this.keyFor({ symbol: this.symbol, transport: this.transport, eventKinds: [] });
    if (this.rowMap.has(key)) return;
    const row: RowState = { key, symbol: this.symbol, transport: this.transport, count: 0 };
    this.rowMap.set(key, row);
    this.rows.set([...this.rowMap.values()]);
    await this.hub.subscribe({
      symbol: this.symbol,
      transport: this.transport,
      eventKinds: this.selectedKindNames(),
    });
  }

  async unsubscribe(row: RowState): Promise<void> {
    await this.hub.unsubscribe(row.symbol, row.transport);
    this.rowMap.delete(row.key);
    this.rows.set([...this.rowMap.values()]);
  }

  private onTick(t: MarketTick): void {
    // Map the legacy `transport` (TCP/UDP/QuickFIX uppercase) back to our enum-cased Transport.
    const transport = (t.transport === 'TCP' ? 'Tcp' : t.transport === 'UDP' ? 'Udp' : 'QuickFix') as Transport;
    const key = this.keyFor({ symbol: t.symbol, transport, eventKinds: [] });
    const row = this.rowMap.get(key);
    if (!row) return;
    row.lastPrice = t.price;
    row.lastQty = t.quantity;
    row.lastEntry = t.entryType;
    row.lastUpdateTicks = t.timestampTicks;
    row.count++;
    this.rows.set([...this.rowMap.values()]);
  }

  private onEvent(e: FixEvent): void {
    if (e.kind === 'MarketDataIncrementalRefresh' || e.kind === 'MarketDataSnapshotFullRefresh') {
      // Already rendered via the legacy "tick" channel.
      return;
    }
    const log = this.sessionLog();
    const next: SessionLogRow = {
      ts: Date.now(),
      kind: e.kind,
      symbol: e.symbol,
      source: e.source,
      detail: JSON.stringify(e.payload ?? {}),
    };
    // Keep last 100 rows.
    this.sessionLog.set([next, ...log].slice(0, 100));
  }

  private keyFor(s: Subscription): string {
    return `${s.symbol}|${s.transport}`;
  }
}
