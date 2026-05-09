import { Subject, HubConnectionState } from "@microsoft/signalr";
import { act, fireEvent, render, waitFor } from "@testing-library/react";
import { useEffect } from "react";
import { describe, expect, it } from "vitest";
import { createSignalRHub, type HubStream } from "../src/index";
import { Deferred, FakeHubConnection, FakeStream } from "./fakes";

interface ServerHub {
  Add(left: number, right: number): Promise<number>;
  Notify(message: string): Promise<void>;
  CountTo(limit: number): HubStream<number>;
  Upload(messages: Subject<string>): Promise<void>;
}

interface ClientEvents {
  Receive(user: string, message: string): void;
  Tick(value: number): void;
}

const {
  SignalRProvider,
  useConnectionState,
  useHubEvent,
  useHubInvoke,
  useHubSend,
  useHubStream,
  useHubSubject,
  useSignalR
} = createSignalRHub<ServerHub, ClientEvents>();

function providerFor(connection: FakeHubConnection) {
  return () => connection.asHubConnection();
}

describe("react-signalr runtime behavior", () => {
  it("auto-starts the connection and exposes stable connection state", async () => {
    const connection = new FakeHubConnection();

    function Probe() {
      const state = useConnectionState();
      return (
        <output data-testid="state">
          {state.state}:{String(state.isConnected)}
        </output>
      );
    }

    const { getByTestId } = render(
      <SignalRProvider connectionFactory={providerFor(connection)}>
        <Probe />
      </SignalRProvider>
    );

    await waitFor(() => {
      expect(getByTestId("state").textContent).toBe("Connected:true");
    });
    expect(connection.startCalls).toBe(1);
  });

  it("supports manual start when autoStart is disabled", async () => {
    const connection = new FakeHubConnection();

    function Controls() {
      const { start, isConnected } = useSignalR();
      return (
        <button onClick={() => void start()}>{String(isConnected)}</button>
      );
    }

    const { getByRole } = render(
      <SignalRProvider
        autoStart={false}
        connectionFactory={providerFor(connection)}
      >
        <Controls />
      </SignalRProvider>
    );

    expect(connection.startCalls).toBe(0);
    expect(getByRole("button").textContent).toBe("false");

    fireEvent.click(getByRole("button"));

    await waitFor(() => {
      expect(getByRole("button").textContent).toBe("true");
    });
    expect(connection.startCalls).toBe(1);
  });

  it("deduplicates concurrent start calls", async () => {
    const connection = new FakeHubConnection();
    const deferred = new Deferred<void>();
    connection.startImplementation = () => deferred.promise;

    function Starter() {
      const { start } = useSignalR();

      useEffect(() => {
        void start();
        void start();
      }, [start]);

      return null;
    }

    render(
      <SignalRProvider
        autoStart={false}
        connectionFactory={providerFor(connection)}
      >
        <Starter />
      </SignalRProvider>
    );

    expect(connection.startCalls).toBe(1);

    await act(async () => {
      deferred.resolve();
      await deferred.promise;
    });
  });

  it("records start failures in connection state", async () => {
    const connection = new FakeHubConnection();
    const failure = new Error("start failed");
    connection.startImplementation = async () => {
      throw failure;
    };

    function Probe() {
      const state = useConnectionState();
      return (
        <output data-testid="state">
          {state.state}:{state.error instanceof Error ? state.error.message : ""}
        </output>
      );
    }

    const { getByTestId } = render(
      <SignalRProvider connectionFactory={providerFor(connection)}>
        <Probe />
      </SignalRProvider>
    );

    await waitFor(() => {
      expect(getByTestId("state").textContent).toBe(
        "Disconnected:start failed"
      );
    });
  });

  it("keeps event handlers fresh without re-registering them and cleans up on unmount", async () => {
    const connection = new FakeHubConnection();
    const events: string[] = [];
    const factory = providerFor(connection);

    function Receiver({ prefix }: { prefix: string }) {
      useHubEvent("Receive", (user, message) => {
        events.push(`${prefix}:${user}:${message}`);
      });
      return null;
    }

    const { rerender, unmount } = render(
      <SignalRProvider connectionFactory={factory}>
        <Receiver prefix="first" />
      </SignalRProvider>
    );

    await waitFor(() => {
      expect(connection.handlerCount("Receive")).toBe(1);
    });

    act(() => {
      connection.emit("Receive", "Ada", "hello");
    });

    rerender(
      <SignalRProvider connectionFactory={factory}>
        <Receiver prefix="second" />
      </SignalRProvider>
    );

    expect(connection.handlerCount("Receive")).toBe(1);

    act(() => {
      connection.emit("Receive", "Grace", "hi");
    });

    unmount();

    expect(connection.handlerCount("Receive")).toBe(0);

    act(() => {
      connection.emit("Receive", "Katherine", "bye");
    });

    expect(events).toEqual(["first:Ada:hello", "second:Grace:hi"]);
  });

  it("provides typed invoke and send helpers", async () => {
    const connection = new FakeHubConnection();
    connection.invokeResults.set("Add", 5);
    const results: number[] = [];

    function Controls() {
      const add = useHubInvoke("Add");
      const notify = useHubSend("Notify");

      return (
        <>
          <button onClick={() => void add(2, 3).then((value) => results.push(value))}>
            add
          </button>
          <button onClick={() => void notify("ready")}>notify</button>
        </>
      );
    }

    const { getByText } = render(
      <SignalRProvider connectionFactory={providerFor(connection)}>
        <Controls />
      </SignalRProvider>
    );

    await waitFor(() => {
      expect(connection.state).toBe(HubConnectionState.Connected);
    });

    fireEvent.click(getByText("add"));
    fireEvent.click(getByText("notify"));

    await waitFor(() => {
      expect(results).toEqual([5]);
    });
    expect(connection.invokeCalls).toEqual([
      { methodName: "Add", args: [2, 3] }
    ]);
    expect(connection.sendCalls).toEqual([
      { methodName: "Notify", args: ["ready"] }
    ]);
  });

  it("tracks reconnecting, reconnected, and closed lifecycle transitions", async () => {
    const connection = new FakeHubConnection();

    function Probe() {
      const state = useConnectionState();
      return (
        <output data-testid="state">
          {state.state}:{state.connectionId ?? ""}
        </output>
      );
    }

    const { getByTestId } = render(
      <SignalRProvider connectionFactory={providerFor(connection)}>
        <Probe />
      </SignalRProvider>
    );

    await waitFor(() => {
      expect(getByTestId("state").textContent).toBe(
        "Connected:fake-connection"
      );
    });

    act(() => {
      connection.triggerReconnecting(new Error("network"));
    });
    expect(getByTestId("state").textContent).toBe(
      "Reconnecting:fake-connection"
    );

    act(() => {
      connection.triggerReconnected("next-id");
    });
    expect(getByTestId("state").textContent).toBe("Connected:next-id");

    act(() => {
      connection.triggerClose(new Error("closed"));
    });
    expect(getByTestId("state").textContent).toBe("Disconnected:");
  });

  it("keeps lifecycle callbacks active after a manual restart", async () => {
    const connection = new FakeHubConnection();

    function Controls() {
      const { restart, state } = useSignalR();
      return <button onClick={() => void restart()}>{state}</button>;
    }

    const { getByRole } = render(
      <SignalRProvider
        autoStart={false}
        connectionFactory={providerFor(connection)}
      >
        <Controls />
      </SignalRProvider>
    );

    fireEvent.click(getByRole("button"));

    await waitFor(() => {
      expect(getByRole("button").textContent).toBe("Connected");
    });

    act(() => {
      connection.triggerReconnecting(new Error("network"));
    });

    expect(getByRole("button").textContent).toBe("Reconnecting");
  });

  it("wraps SignalR streaming with typed stream starters", async () => {
    const connection = new FakeHubConnection();
    const stream = new FakeStream<number>();
    connection.streams.set("CountTo", stream as FakeStream<unknown>);
    const items: number[] = [];
    let completed = 0;

    function StreamConsumer() {
      const { isConnected } = useConnectionState();
      const countTo = useHubStream("CountTo");

      if (!isConnected) {
        return null;
      }

      return <button onClick={() => {
        const subscription = countTo(3).subscribe({
          next: (value) => items.push(value),
          error: () => undefined,
          complete: () => {
            completed += 1;
          }
        });
        subscription.dispose();
        countTo(3).subscribe({
          next: (value) => items.push(value),
          error: () => undefined,
          complete: () => {
            completed += 1;
          }
        });
      }}>stream</button>;
    }

    const { getByText } = render(
      <SignalRProvider connectionFactory={providerFor(connection)}>
        <StreamConsumer />
      </SignalRProvider>
    );

    await waitFor(() => {
      expect(getByText("stream")).toBeTruthy();
    });

    fireEvent.click(getByText("stream"));

    expect(connection.streamCalls).toEqual([
      { methodName: "CountTo", args: [3] },
      { methodName: "CountTo", args: [3] }
    ]);
    expect(stream.disposeCount).toBe(1);

    act(() => {
      stream.next(1);
      stream.next(2);
      stream.complete();
    });

    expect(items).toEqual([1, 2]);
    expect(completed).toBe(1);
  });

  it("completes hub subjects on unmount", async () => {
    const connection = new FakeHubConnection();
    let completed = 0;

    function SubjectConsumer() {
      const subject = useHubSubject<string>();

      useEffect(() => {
        subject.subscribe({
          next: () => undefined,
          error: () => undefined,
          complete: () => {
            completed += 1;
          }
        });
      }, [subject]);

      return null;
    }

    const { unmount } = render(
      <SignalRProvider
        autoStart={false}
        connectionFactory={providerFor(connection)}
      >
        <SubjectConsumer />
      </SignalRProvider>
    );

    unmount();

    expect(completed).toBe(1);
  });
});
