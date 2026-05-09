import * as signalR from "@microsoft/signalr";
import type {
  HubConnection,
  IHttpConnectionOptions,
  IRetryPolicy,
  IStreamResult
} from "@microsoft/signalr";
import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
  type ReactNode
} from "react";

type AnyHubFunction = (...args: any[]) => any;

type MethodName<TContract> = Extract<
  {
    [TKey in keyof TContract]: TContract[TKey] extends AnyHubFunction
      ? TKey
      : never;
  }[keyof TContract],
  string
>;

type MethodArgs<
  TContract,
  TMethod extends MethodName<TContract>
> = TContract[TMethod] extends AnyHubFunction
  ? Parameters<TContract[TMethod]>
  : never;

type MethodReturn<
  TContract,
  TMethod extends MethodName<TContract>
> = TContract[TMethod] extends AnyHubFunction
  ? ReturnType<TContract[TMethod]>
  : never;

type IsNever<TValue> = [TValue] extends [never] ? true : false;

type StreamItem<TValue> = TValue extends IStreamResult<infer TItem>
  ? TItem
  : TValue extends AsyncIterable<infer TItem>
    ? TItem
    : never;

type StreamMethodName<TContract> = Extract<
  {
    [TKey in MethodName<TContract>]: IsNever<
      StreamItem<Awaited<MethodReturn<TContract, TKey>>>
    > extends true
      ? never
      : TKey;
  }[MethodName<TContract>],
  string
>;

type InvocationMethodName<TContract> = Exclude<
  MethodName<TContract>,
  StreamMethodName<TContract>
>;

type SendMethodName<TContract> = Extract<
  {
    [TKey in InvocationMethodName<TContract>]: Awaited<
      MethodReturn<TContract, TKey>
    > extends void
      ? TKey
      : never;
  }[InvocationMethodName<TContract>],
  string
>;

export type HubStream<TItem> = IStreamResult<TItem> | AsyncIterable<TItem>;

export interface SignalRConnectionConfig {
  url?: string | (() => string);
  options?: IHttpConnectionOptions;
  connectionFactory?: () => HubConnection;
  autoStart?: boolean;
  reconnect?: boolean | number[] | IRetryPolicy;
  connectionKey?: string | number;
}

export interface SignalRProviderProps extends SignalRConnectionConfig {
  children: ReactNode;
}

export interface SignalRConnectionSnapshot {
  readonly connection: HubConnection | null;
  readonly state: signalR.HubConnectionState;
  readonly connectionId: string | null;
  readonly error: unknown;
  readonly isConnecting: boolean;
  readonly isConnected: boolean;
  readonly isReconnecting: boolean;
  readonly isDisconnected: boolean;
}

export interface SignalRHubControls<TServerHub extends object>
  extends SignalRConnectionSnapshot {
  start: () => Promise<void>;
  stop: () => Promise<void>;
  restart: () => Promise<void>;
  invoke: <TMethod extends InvocationMethodName<TServerHub>>(
    methodName: TMethod,
    ...args: MethodArgs<TServerHub, TMethod>
  ) => Promise<Awaited<MethodReturn<TServerHub, TMethod>>>;
  send: <TMethod extends SendMethodName<TServerHub>>(
    methodName: TMethod,
    ...args: MethodArgs<TServerHub, TMethod>
  ) => Promise<void>;
  stream: <TMethod extends StreamMethodName<TServerHub>>(
    methodName: TMethod,
    ...args: MethodArgs<TServerHub, TMethod>
  ) => IStreamResult<StreamItem<Awaited<MethodReturn<TServerHub, TMethod>>>>;
}

interface InternalSignalRContext<TServerHub extends object> {
  readonly store: SignalRStore;
  readonly start: () => Promise<void>;
  readonly stop: () => Promise<void>;
  readonly restart: () => Promise<void>;
  readonly invoke: SignalRHubControls<TServerHub>["invoke"];
  readonly send: SignalRHubControls<TServerHub>["send"];
  readonly stream: SignalRHubControls<TServerHub>["stream"];
}

type Listener = () => void;

const disconnectedState = signalR.HubConnectionState.Disconnected;

function toSnapshot(
  snapshot: Omit<
    SignalRConnectionSnapshot,
    "isConnecting" | "isConnected" | "isReconnecting" | "isDisconnected"
  >
): SignalRConnectionSnapshot {
  return {
    ...snapshot,
    isConnecting: snapshot.state === signalR.HubConnectionState.Connecting,
    isConnected: snapshot.state === signalR.HubConnectionState.Connected,
    isReconnecting: snapshot.state === signalR.HubConnectionState.Reconnecting,
    isDisconnected: snapshot.state === disconnectedState
  };
}

class SignalRStore {
  private listeners = new Set<Listener>();

  private snapshot: SignalRConnectionSnapshot = toSnapshot({
    connection: null,
    state: disconnectedState,
    connectionId: null,
    error: undefined
  });

  getSnapshot = () => this.snapshot;

  subscribe = (listener: Listener) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  update(
    patch: Partial<
      Omit<
        SignalRConnectionSnapshot,
        "isConnecting" | "isConnected" | "isReconnecting" | "isDisconnected"
      >
    >
  ) {
    const has = (key: keyof typeof patch) =>
      Object.prototype.hasOwnProperty.call(patch, key);

    const next = toSnapshot({
      connection: has("connection")
        ? (patch.connection ?? null)
        : this.snapshot.connection,
      state: has("state")
        ? (patch.state ?? disconnectedState)
        : this.snapshot.state,
      connectionId: has("connectionId")
        ? (patch.connectionId ?? null)
        : this.snapshot.connectionId,
      error: has("error") ? patch.error : this.snapshot.error
    });

    if (
      next.connection === this.snapshot.connection &&
      next.state === this.snapshot.state &&
      next.connectionId === this.snapshot.connectionId &&
      next.error === this.snapshot.error
    ) {
      return;
    }

    this.snapshot = next;
    for (const listener of this.listeners) {
      listener();
    }
  }
}

function noop() {}

function missingConnectionError() {
  return new Error(
    "No SignalR connection is available. Provide `url` or `connectionFactory` to SignalRProvider."
  );
}

function buildConnection(props: SignalRConnectionConfig) {
  if (props.connectionFactory) {
    return props.connectionFactory();
  }

  if (!props.url) {
    throw missingConnectionError();
  }

  const url = typeof props.url === "function" ? props.url() : props.url;
  const builder = new signalR.HubConnectionBuilder();

  if (props.options) {
    builder.withUrl(url, props.options);
  } else {
    builder.withUrl(url);
  }

  const reconnect = props.reconnect ?? true;

  if (reconnect === true) {
    builder.withAutomaticReconnect();
  } else if (Array.isArray(reconnect)) {
    builder.withAutomaticReconnect(reconnect);
  } else if (reconnect) {
    builder.withAutomaticReconnect(reconnect);
  }

  return builder.build();
}

function connectionIdOf(connection: HubConnection) {
  return connection.connectionId ?? null;
}

function useLatest<TValue>(value: TValue) {
  const ref = useRef(value);
  ref.current = value;
  return ref;
}

export function createSignalRHub<
  TServerHub extends object,
  TClientEvents extends object = Record<never, never>
>() {
  const SignalRContext =
    createContext<InternalSignalRContext<TServerHub> | null>(null);

  function useRequiredContext() {
    const context = useContext(SignalRContext);

    if (!context) {
      throw new Error(
        "SignalR hooks must be used inside the matching SignalRProvider."
      );
    }

    return context;
  }

  function useSnapshot() {
    const context = useRequiredContext();
    return useSyncExternalStore(
      context.store.subscribe,
      context.store.getSnapshot,
      context.store.getSnapshot
    );
  }

  function SignalRProvider({
    children,
    ...props
  }: SignalRProviderProps) {
    const storeRef = useRef<SignalRStore | null>(null);
    const connectionRef = useRef<HubConnection | null>(null);
    const startPromiseRef = useRef<Promise<void> | null>(null);
    const generationRef = useRef(0);
    const propsRef = useRef(props);

    propsRef.current = props;

    if (!storeRef.current) {
      storeRef.current = new SignalRStore();
    }

    const store = storeRef.current;

    const wireConnection = useCallback(
      (connection: HubConnection, generation: number) => {
        const isCurrent = () =>
          generationRef.current === generation &&
          connectionRef.current === connection;

        connection.onreconnecting((error) => {
          if (!isCurrent()) {
            return;
          }

          store.update({
            connection,
            state: signalR.HubConnectionState.Reconnecting,
            connectionId: connectionIdOf(connection),
            error
          });
        });

        connection.onreconnected((connectionId) => {
          if (!isCurrent()) {
            return;
          }

          store.update({
            connection,
            state: signalR.HubConnectionState.Connected,
            connectionId: connectionId ?? connectionIdOf(connection),
            error: undefined
          });
        });

        connection.onclose((error) => {
          if (!isCurrent()) {
            return;
          }

          store.update({
            connection,
            state: disconnectedState,
            connectionId: null,
            error
          });
        });
      },
      [store]
    );

    const replaceConnection = useCallback(() => {
      const previous = connectionRef.current;
      const generation = generationRef.current + 1;

      generationRef.current = generation;
      startPromiseRef.current = null;
      connectionRef.current = null;

      if (previous && previous.state !== disconnectedState) {
        previous.stop().catch(noop);
      }

      try {
        const connection = buildConnection(propsRef.current);
        connectionRef.current = connection;
        wireConnection(connection, generation);
        store.update({
          connection,
          state: connection.state,
          connectionId: connectionIdOf(connection),
          error: undefined
        });
        return connection;
      } catch (error) {
        store.update({
          connection: null,
          state: disconnectedState,
          connectionId: null,
          error
        });
        return null;
      }
    }, [store, wireConnection]);

    const ensureConnection = useCallback(() => {
      return connectionRef.current ?? replaceConnection();
    }, [replaceConnection]);

    const start = useCallback(async () => {
      if (startPromiseRef.current) {
        return startPromiseRef.current;
      }

      const connection = ensureConnection();

      if (!connection) {
        throw missingConnectionError();
      }

      if (
        connection.state === signalR.HubConnectionState.Connected ||
        connection.state === signalR.HubConnectionState.Connecting ||
        connection.state === signalR.HubConnectionState.Reconnecting
      ) {
        store.update({
          connection,
          state: connection.state,
          connectionId: connectionIdOf(connection),
          error: undefined
        });
        return;
      }

      const generation = generationRef.current;

      store.update({
        connection,
        state: signalR.HubConnectionState.Connecting,
        connectionId: connectionIdOf(connection),
        error: undefined
      });

      const startPromise = connection
        .start()
        .then(() => {
          if (
            generationRef.current !== generation ||
            connectionRef.current !== connection
          ) {
            return;
          }

          store.update({
            connection,
            state: connection.state,
            connectionId: connectionIdOf(connection),
            error: undefined
          });
        })
        .catch((error: unknown) => {
          if (
            generationRef.current === generation &&
            connectionRef.current === connection
          ) {
            store.update({
              connection,
              state: disconnectedState,
              connectionId: null,
              error
            });
          }

          throw error;
        })
        .finally(() => {
          if (startPromiseRef.current === startPromise) {
            startPromiseRef.current = null;
          }
        });

      startPromiseRef.current = startPromise;
      return startPromise;
    }, [ensureConnection, store]);

    const stop = useCallback(async () => {
      const connection = connectionRef.current;

      if (!connection) {
        store.update({
          connection: null,
          state: disconnectedState,
          connectionId: null,
          error: undefined
        });
        return;
      }

      startPromiseRef.current = null;

      if (connection.state === disconnectedState) {
        store.update({
          connection,
          state: disconnectedState,
          connectionId: null,
          error: undefined
        });
        return;
      }

      store.update({
        connection,
        state: signalR.HubConnectionState.Disconnecting,
        connectionId: connectionIdOf(connection)
      });

      try {
        await connection.stop();
        store.update({
          connection,
          state: disconnectedState,
          connectionId: null,
          error: undefined
        });
      } catch (error) {
        store.update({
          connection,
          state: connection.state,
          connectionId: connectionIdOf(connection),
          error
        });
        throw error;
      }
    }, [store]);

    const restart = useCallback(async () => {
      await stop();
      await start();
    }, [start, stop]);

    const invoke = useCallback(
      <TMethod extends InvocationMethodName<TServerHub>>(
        methodName: TMethod,
        ...args: MethodArgs<TServerHub, TMethod>
      ) => {
        const connection = connectionRef.current;

        if (!connection) {
          return Promise.reject(missingConnectionError());
        }

        return connection.invoke(
          methodName,
          ...args
        ) as Promise<Awaited<MethodReturn<TServerHub, TMethod>>>;
      },
      []
    );

    const send = useCallback(
      <TMethod extends SendMethodName<TServerHub>>(
        methodName: TMethod,
        ...args: MethodArgs<TServerHub, TMethod>
      ) => {
        const connection = connectionRef.current;

        if (!connection) {
          return Promise.reject(missingConnectionError());
        }

        return connection.send(methodName, ...args);
      },
      []
    );

    const stream = useCallback(
      <TMethod extends StreamMethodName<TServerHub>>(
        methodName: TMethod,
        ...args: MethodArgs<TServerHub, TMethod>
      ) => {
        const connection = connectionRef.current;

        if (!connection) {
          throw missingConnectionError();
        }

        return connection.stream(
          methodName,
          ...args
        ) as IStreamResult<
          StreamItem<Awaited<MethodReturn<TServerHub, TMethod>>>
        >;
      },
      []
    );

    useEffect(() => {
      const connection = replaceConnection();

      return () => {
        generationRef.current += 1;
        startPromiseRef.current = null;

        if (connection && connectionRef.current === connection) {
          connectionRef.current = null;
        }

        store.update({
          connection: null,
          state: disconnectedState,
          connectionId: null
        });

        if (connection && connection.state !== disconnectedState) {
          connection.stop().catch(noop);
        }
      };
    }, [
      props.connectionFactory,
      props.connectionKey,
      props.options,
      props.reconnect,
      props.url,
      replaceConnection,
      store
    ]);

    useEffect(() => {
      if (props.autoStart === false) {
        return;
      }

      start().catch(noop);
    }, [
      props.autoStart,
      props.connectionFactory,
      props.connectionKey,
      props.options,
      props.reconnect,
      props.url,
      start
    ]);

    const value = useMemo<InternalSignalRContext<TServerHub>>(
      () => ({
        store,
        start,
        stop,
        restart,
        invoke,
        send,
        stream
      }),
      [invoke, restart, send, start, stop, store, stream]
    );

    return createElement(SignalRContext.Provider, { value }, children);
  }

  function useConnectionState() {
    return useSnapshot();
  }

  function useSignalR(): SignalRHubControls<TServerHub> {
    const context = useRequiredContext();
    const snapshot = useSyncExternalStore(
      context.store.subscribe,
      context.store.getSnapshot,
      context.store.getSnapshot
    );

    return useMemo(
      () => ({
        ...snapshot,
        start: context.start,
        stop: context.stop,
        restart: context.restart,
        invoke: context.invoke,
        send: context.send,
        stream: context.stream
      }),
      [context, snapshot]
    );
  }

  function useHubEvent<TEvent extends MethodName<TClientEvents>>(
    eventName: TEvent,
    handler: (
      ...args: MethodArgs<TClientEvents, TEvent>
    ) => void
  ) {
    const latestHandler = useLatest(handler);
    const { connection } = useConnectionState();

    useEffect(() => {
      if (!connection) {
        return;
      }

      const listener = (...args: unknown[]) => {
        latestHandler.current(
          ...(args as MethodArgs<TClientEvents, TEvent>)
        );
      };

      connection.on(eventName, listener);

      return () => {
        connection.off(eventName, listener);
      };
    }, [connection, eventName, latestHandler]);
  }

  function useHubInvoke<TMethod extends InvocationMethodName<TServerHub>>(
    methodName: TMethod
  ) {
    const context = useRequiredContext();

    return useCallback(
      (...args: MethodArgs<TServerHub, TMethod>) =>
        context.invoke(methodName, ...args),
      [context, methodName]
    );
  }

  function useHubSend<TMethod extends SendMethodName<TServerHub>>(
    methodName: TMethod
  ) {
    const context = useRequiredContext();

    return useCallback(
      (...args: MethodArgs<TServerHub, TMethod>) =>
        context.send(methodName, ...args),
      [context, methodName]
    );
  }

  function useHubStream<TMethod extends StreamMethodName<TServerHub>>(
    methodName: TMethod
  ) {
    const context = useRequiredContext();

    return useCallback(
      (...args: MethodArgs<TServerHub, TMethod>) =>
        context.stream(methodName, ...args),
      [context, methodName]
    );
  }

  function useHubSubject<TItem>() {
    const subjectRef = useRef<signalR.Subject<TItem> | null>(null);

    if (!subjectRef.current) {
      subjectRef.current = new signalR.Subject<TItem>();
    }

    useEffect(() => {
      const subject = subjectRef.current;

      return () => {
        subject?.complete();
      };
    }, []);

    return subjectRef.current;
  }

  return {
    SignalRProvider,
    useSignalR,
    useConnectionState,
    useHubEvent,
    useHubInvoke,
    useHubSend,
    useHubStream,
    useHubSubject
  };
}
