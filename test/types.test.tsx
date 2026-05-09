import { expectTypeOf } from "vitest";
import { Subject, type IStreamResult } from "@microsoft/signalr";
import { createSignalRHub, type HubStream } from "../src/index";

interface ServerHub {
  Add(left: number, right: number): Promise<number>;
  Notify(message: string): Promise<void>;
  CountTo(limit: number): HubStream<number>;
  RawStream(room: string): IStreamResult<{ value: string }>;
  Upload(messages: Subject<string>): Promise<void>;
}

interface ClientEvents {
  Receive(user: string, message: string): void;
  Tick(value: number): void;
}

const {
  useHubEvent,
  useHubInvoke,
  useHubSend,
  useHubStream,
  useHubSubject,
  useSignalR
} = createSignalRHub<ServerHub, ClientEvents>();

function TypeContract() {
  const hub = useSignalR();
  expectTypeOf(hub.isConnected).toEqualTypeOf<boolean>();
  expectTypeOf(hub.invoke("Add", 1, 2)).toEqualTypeOf<Promise<number>>();

  const add = useHubInvoke("Add");
  expectTypeOf(add).toEqualTypeOf<
    (left: number, right: number) => Promise<number>
  >();

  const notify = useHubSend("Notify");
  expectTypeOf(notify).toEqualTypeOf<(message: string) => Promise<void>>();

  const countTo = useHubStream("CountTo");
  expectTypeOf(countTo).toEqualTypeOf<
    (limit: number) => IStreamResult<number>
  >();

  const rawStream = useHubStream("RawStream");
  expectTypeOf(rawStream).toEqualTypeOf<
    (room: string) => IStreamResult<{ value: string }>
  >();

  const subject = useHubSubject<string>();
  expectTypeOf(subject).toEqualTypeOf<Subject<string>>();

  useHubEvent("Receive", (user, message) => {
    expectTypeOf(user).toEqualTypeOf<string>();
    expectTypeOf(message).toEqualTypeOf<string>();
  });

  // @ts-expect-error unknown server method
  useHubInvoke("Missing");

  // @ts-expect-error invalid invoke argument type
  add("1", 2);

  // @ts-expect-error streaming methods use useHubStream
  useHubInvoke("CountTo");

  // @ts-expect-error send is only for fire-and-forget methods
  useHubSend("Add");

  // @ts-expect-error non-streaming methods cannot be streamed
  useHubStream("Add");

  // @ts-expect-error unknown client event
  useHubEvent("Missing", () => undefined);

  // @ts-expect-error client event payload is strongly typed
  useHubEvent("Tick", (value: string) => {
    value.toUpperCase();
  });

  return null;
}

void TypeContract;
