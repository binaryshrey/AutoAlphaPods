import { Suspense } from "react";
import ChatPageContent from "@/components/chat-page";

export default function ChatPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen bg-[#080808] flex items-center justify-center">
          <div className="text-zinc-500 text-sm">Loading…</div>
        </div>
      }
    >
      <ChatPageContent />
    </Suspense>
  );
}
