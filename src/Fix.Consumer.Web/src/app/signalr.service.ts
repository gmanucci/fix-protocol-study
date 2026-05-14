import { Injectable, signal } from '@angular/core';
import * as signalR from '@microsoft/signalr';
import { Subject } from 'rxjs';
import { MarketTick, Transport } from './market.models';

/**
 * Wraps the SignalR `HubConnectionBuilder`. Exposes a `tick$` stream and a connection
 * status signal so components can react to lifecycle changes.
 */
@Injectable({ providedIn: 'root' })
export class SignalrService {
  private hub?: signalR.HubConnection;
  readonly tick$ = new Subject<MarketTick>();
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
      this.hub.onreconnected(() => this.connected.set(true));
      this.hub.onclose(() => this.connected.set(false));
    }
    if (this.hub.state === signalR.HubConnectionState.Disconnected) {
      await this.hub.start();
      this.connected.set(true);
    }
  }

  async subscribe(symbol: string, transport: Transport): Promise<void> {
    await this.ensureConnected();
    await this.hub!.invoke('Subscribe', symbol, transport);
  }

  async unsubscribe(symbol: string, transport: Transport): Promise<void> {
    if (!this.hub) return;
    await this.hub.invoke('Unsubscribe', symbol, transport);
  }
}
