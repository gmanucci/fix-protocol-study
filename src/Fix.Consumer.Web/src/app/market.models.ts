/** Tick payload sent by the SignalR hub for backwards compatibility. Mirrors `TickDto`. */
export interface MarketTick {
  symbol: string;
  price: number;
  quantity: number;
  timestampTicks: number;
  entryType: 'Bid' | 'Offer' | 'Trade' | 'Unknown';
  transport: 'TCP' | 'UDP' | 'QuickFIX';
}

/** Generic FIX event envelope sent by the hub on the `"event"` channel. Mirrors `FixEvent`. */
export interface FixEvent {
  kind: string;
  symbol: string | null;
  senderCompId: string | null;
  targetCompId: string | null;
  msgSeqNum: number | null;
  sendingTimeTicks: number | null;
  source: string;
  payload: any | null;
}

export type Transport = 'Tcp' | 'Udp' | 'QuickFix';

export interface Subscription {
  symbol: string;
  transport: Transport;
  eventKinds: string[];
}

export interface SubscriptionRequest {
  symbol: string;
  transport: Transport;
  eventKinds: string[];
}
