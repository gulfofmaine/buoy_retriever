import Link from "next/link";

export default function Home() {
  return (
    <div>
      <main>
        <h1>Buoy Retriever</h1>
        <Link href="/manage">Manage datasets</Link>
      </main>
    </div>
  );
}
