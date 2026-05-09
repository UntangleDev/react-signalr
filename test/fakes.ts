import {
  HubConnectionState,
  type HubConnection,
  type IStreamResult,
  type IStreamSubscriber,
  type ISubscription
} from "@microsoft/signalr";

export class Deferred<TValue = void> {
  promise: Promise<TValue>;
  resolve!: (value: TValue | PromiseLike<TValue>) => void;
  reject!: (reason?: unknown) => void;

  constructor() {
    this.promise = new Promise<TValue>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

export class FakeStream<TItem> implements IStreamResult<TItem> {
  subscribers = new Set<IStreamSubscriber<TItem>>();
  disposeCount = 0;

  subscribe(subscriber: IStreamSubscriber<TItem>): ISubscription<TItem> {
    this.subscribers.add(subscriber);

    return {
      dispose: () => {
        this.disposeCount += 1;
        subscriber.closed = true;
        this.subscribers.delete(subscriber);
      }
    };
  }

  next(value: TItem) {
    for (const subscriber of this.subscribers) {
      if (!subscriber.closed) {
        subscriber.next(value);
      }
    }
  }

  error(error: unknown) {
    for (const subscriber of this.subscribers) {
      if (!subscriber.closed) {
        subscriber.closed = true;
        subscriber.error(error);
      }
    }

    this.subscribers.clear();
  }

  complete() {
    for (const subscriber of this.subscribers) {
      if (!subscriber.closed) {
        subscriber.closed = true;
        subscriber.complete();
      }
    }

    this.subscribers.clear();
  }
}

type Handler = (...args: any[]) => void;

export class FakeHubConnection {
  state = HubConnectionState.Disconnected;
  connectionId: string | null = null;
  startCalls = 0;
  stopCalls = 0;
  invokeCalls: Array<{ methodName: string; args: unknown[] }> = [];
  sendCalls: Array<{ methodName: string; args: unknown[] }> = [];
  streamCalls: Array<{ methodName: string; args: unknown[] }> = [];
  invokeResults = new Map<string, unknown>();
  streams = new Map<string, FakeStream<unknown>>();
  startImplementation?: () => Promise<void>;

  private handlers = new Map<string, Set<Handler>>();
  private closeCallbacks = new Set<(error?: Error) => void>();
  private reconnectingCallbacks = new Set<(error?: Error) => void>();
  private reconnectedCallbacks = new Set<(connectionId?: string) => void>();

  asHubConnection() {
    return this as unknown as HubConnection;
  }

  async start() {
    this.startCalls += 1;
    this.state = HubConnectionState.Connecting;

    try {
      await this.startImplementation?.();
      this.state = HubConnectionState.Connected;
      this.connectionId = this.connectionId ?? "fake-connection";
    } catch (error) {
      this.state = HubConnectionState.Disconnected;
      this.connectionId = null;
      throw error;
    }
  }

  async stop() {
    this.stopCalls += 1;
    this.state = HubConnectionState.Disconnected;
    this.connectionId = null;
  }

  invoke<TValue = unknown>(methodName: string, ...args: unknown[]) {
    this.invokeCalls.push({ methodName, args });
    const result = this.invokeResults.get(methodName);

    if (result instanceof Error) {
      return Promise.reject(result);
    }

    return Promise.resolve(result as TValue);
  }

  send(methodName: string, ...args: unknown[]) {
    this.sendCalls.push({ methodName, args });
    return Promise.resolve();
  }

  stream<TItem = unknown>(methodName: string, ...args: unknown[]) {
    this.streamCalls.push({ methodName, args });

    const existing = this.streams.get(methodName);
    if (existing) {
      return existing as FakeStream<TItem>;
    }

    const stream = new FakeStream<TItem>();
    this.streams.set(methodName, stream as FakeStream<unknown>);
    return stream;
  }

  on(methodName: string, handler: Handler) {
    const handlers = this.handlers.get(methodName) ?? new Set<Handler>();
    handlers.add(handler);
    this.handlers.set(methodName, handlers);
  }

  off(methodName: string, handler?: Handler) {
    if (!handler) {
      this.handlers.delete(methodName);
      return;
    }

    const handlers = this.handlers.get(methodName);
    handlers?.delete(handler);

    if (handlers?.size === 0) {
      this.handlers.delete(methodName);
    }
  }

  onclose(callback: (error?: Error) => void) {
    this.closeCallbacks.add(callback);
  }

  onreconnecting(callback: (error?: Error) => void) {
    this.reconnectingCallbacks.add(callback);
  }

  onreconnected(callback: (connectionId?: string) => void) {
    this.reconnectedCallbacks.add(callback);
  }

  emit(methodName: string, ...args: unknown[]) {
    for (const handler of this.handlers.get(methodName) ?? []) {
      handler(...args);
    }
  }

  handlerCount(methodName: string) {
    return this.handlers.get(methodName)?.size ?? 0;
  }

  triggerReconnecting(error?: Error) {
    this.state = HubConnectionState.Reconnecting;

    for (const callback of this.reconnectingCallbacks) {
      callback(error);
    }
  }

  triggerReconnected(connectionId = "fake-reconnected") {
    this.state = HubConnectionState.Connected;
    this.connectionId = connectionId;

    for (const callback of this.reconnectedCallbacks) {
      callback(connectionId);
    }
  }

  triggerClose(error?: Error) {
    this.state = HubConnectionState.Disconnected;
    this.connectionId = null;

    for (const callback of this.closeCallbacks) {
      callback(error);
    }
  }
}
