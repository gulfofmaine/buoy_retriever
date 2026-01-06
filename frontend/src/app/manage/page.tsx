"use client";
import Link from "next/link";

import { useDatasets } from "@/hooks/queries";

export default function ManagePage() {
  const { data, error, isLoading } = useDatasets();

  if (isLoading) return <div>Loading...</div>;
  if (error) {
    // debugger
    return <div>Error loading datasets</div>;
  }

  console.log(data);

  return (
    <div>
      <main>
        <h1>Manage datasets</h1>
        <h2>Hello</h2>

        <Link href="/manage/new">Add new dataset</Link>

        <h2>Existing datasets</h2>

        <ul>
          {data.map((dataset: { slug: string }) => (
            <li key={dataset.slug}>
              <Link href={`/manage/dataset/${dataset.slug}`}>
                {dataset.slug}
              </Link>
            </li>
          ))}
        </ul>
      </main>
    </div>
  );
}
