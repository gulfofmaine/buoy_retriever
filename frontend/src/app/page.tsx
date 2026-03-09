import Link from "next/link";

import { Button } from "@/components/ui/button";

export default function Home() {
  return (
    <main className="min-h-screen flex items-center justify-center p-8">
      <div className="text-center space-y-4">
        <h1>IOOS Buoy Retriever</h1>
        <Button asChild className="h-auto p-4">
          <Link href="/manage">Manage datasets</Link>
        </Button>
      </div>
    </main>
  );
}
