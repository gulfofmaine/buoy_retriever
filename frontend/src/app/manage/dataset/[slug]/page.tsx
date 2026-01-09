"use client";
import Link from "next/link";
import { use } from "react";

import { useDataset } from "@/hooks/queries";

export default function Dataset({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = use(params);
  const { data, error, isError, isPending } = useDataset(slug);

  if (isPending) return <div>Loading...</div>;
  if (isError) return <div>Error: {error.message}</div>;

  return (
    <div>
      <main>
        <h1>Dataset: {slug}</h1>
        Configs:
        <ul>
          {data.configs.map((config) => (
            <li key={config.id}>
              <Link href={`/manage/dataset/${slug}/config/${config.id}`}>
                ID: {config.id}, State: {config.state}
              </Link>{" "}
              Created: {new Date(config.created).toLocaleString()}, Updated:{" "}
              {new Date(config.edited).toLocaleString()}
            </li>
          ))}
        </ul>
      </main>
    </div>
  );
}
