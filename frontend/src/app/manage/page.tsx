"use client";
import Link from "next/link";

import { useDatasets } from "@/hooks/queries";

export default function ManagePage() {
  const { data, isError, isPending } = useDatasets();

  if (isPending) return <div>Loading...</div>;
  if (isError) {
    return <div>Error loading datasets</div>;
  }

  return (
    <div>
      <main>
        <h1>Manage datasets</h1>

        <Link href="/manage/new">Add new dataset</Link>

        <h2>Existing datasets</h2>

        <ul>
          {data?.length > 0 ? (
            data.map((dataset: { slug: string }) => (
              <li key={dataset.slug}>
                <Link href={`/manage/dataset/${dataset.slug}`}>
                  {dataset.slug}
                </Link>
              </li>
            ))
          ) : (
            <li>No datasets available</li>
          )}
        </ul>
      </main>
    </div>
  );
}
