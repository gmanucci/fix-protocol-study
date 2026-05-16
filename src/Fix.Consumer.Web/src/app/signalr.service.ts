import { Injectable, signal } from '@angular/core';
import * as signalR from '@microsoft/signalr';
import { Subject } from 'rxjs';
import { FixEvent, MarketTick, SubscriptionRequest, Transport } from './market.models';

/**
 * Wraps the SignalR `HubConnectionBuilder`. Exposes streams for both the legacy
 * `tick` channel (market-data ticks) and the new generic `event` channel
 * (any subscribed FIX event kind: session, market-data, reject, ...).
 */
@Injectable({ providedIn: 'root' })
export class SignalrService {
  private hub?: signalR.HubConnection;
  readonly tick$ = new Subject<MarketTick>();
  readonly event$ = new Subject<FixEvent>();
  readonly connected = signal(false);

  /** Lazily creates the hub connection on first use. */
  async ensureConnected(hubUrl = '/hub/market'): Promise<void> {
    if (this.hub && this.hub.state === signalR.HubConnectionState.Connected) {
      return;
    }
    if (!this.hub) {
      this.hub = new signalR.HubConnectionBuilder()
        .withUrl(hubUrl)
        .withAutomaticReconnect()
        .configureLogging(signalR.LogLevel.Warning)
        .build();

      this.hub.on('tick', (t: MarketTick) => this.tick$.next(t));
      this.hub.on('event', (e: FixEvent) => this.event$.next(e));
      this.hub.onreconnected(() => this.connected.set(true));
      this.hub.onclose(() => this.connected.set(false));
    }
    if (this.hub.state === signalR.HubConnectionState.Disconnected) {
      await this.hub.start();
      this.connected.set(true);
    }
  }

  /** Returns every FixEventKind name the server can emit. */
  async listEventKinds(): Promise<string[]> {
    await this.ensureConnected();
    return this.hub!.invoke<string[]>('ListEventKinds');
  }

  async subscribe(req: SubscriptionRequest): Promise<void> {
    await this.ensureConnected();
    await this.hub!.invoke('Subscribe', req);
  }

  async unsubscribe(symbol: string, transport: Transport): Promise<void> {
    if (!this.hub) return;
    await this.hub.invoke('Unsubscribe', symbol, transport);
  }
}
