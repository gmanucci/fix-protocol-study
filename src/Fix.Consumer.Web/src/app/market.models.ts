/** Tick payload sent by the SignalR hub. Mirrors `TickDto` on the .NET side. */
export interface MarketTick {
  symbol: string;
  price: number;
  quantity: number;
  timestampTicks: number;
  entryType: 'Bid' | 'Offer' | 'Trade' | 'Unknown';
  transport: 'TCP' | 'UDP';
}

export type Transport = 'TCP' | 'UDP';

export interface Subscription {
  symbol: string;
  transport: Transport;
}
