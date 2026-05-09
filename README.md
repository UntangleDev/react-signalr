# react-signalr

Typed, headless React hooks for ASP.NET Core SignalR.

```tsx
import { createSignalRHub, type HubStream } from "react-signalr";

interface ServerHub {
  SendMessage(room: string, message: string): Promise<void>;
  CountTo(limit: number): HubStream<number>;
}

interface ClientEvents {
  ReceiveMessage(user: string, message: string): void;
}

export const {
  SignalRProvider,
  useConnectionState,
  useHubEvent,
  useHubInvoke,
  useHubStream,
  useHubSubject,
  useSignalR
} = createSignalRHub<ServerHub, ClientEvents>();
```

```tsx
function App() {
  return (
    <SignalRProvider url="/hubs/chat">
      <ChatRoom />
    </SignalRProvider>
  );
}

function ChatRoom() {
  const sendMessage = useHubInvoke("SendMessage");
  const streamCount = useHubStream("CountTo");
  const { isConnected } = useConnectionState();

  useHubEvent("ReceiveMessage", (user, message) => {
    console.log(user, message);
  });

  async function send() {
    await sendMessage("general", "Hello");
  }

  function startStream() {
    const subscription = streamCount(10).subscribe({
      next: (value) => console.log(value),
      complete: () => console.log("done"),
      error: console.error
    });

    return () => subscription.dispose();
  }

  return <button disabled={!isConnected} onClick={send}>Send</button>;
}
```

## Provider options

`SignalRProvider` accepts a simple `url` or a `connectionFactory` escape hatch:

```tsx
<SignalRProvider
  url={() => "/hubs/chat"}
  options={{ accessTokenFactory: getToken }}
  reconnect={[0, 2000, 10000, 30000]}
  connectionKey={userId}
/>
```

Use `connectionKey` when a logical connection should be rebuilt. Keep object and function props stable between renders, as changing them creates a new SignalR connection.

## Testing

The package includes unit tests using fake SignalR connections and type-level tests for method, event, and stream inference.

```sh
npm run typecheck
npm test
npm run build
npm run pack:dry
```

