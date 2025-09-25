"use client";
import { useQuery } from "@tanstack/react-query";
import Link from "next/link";

async function fetchDatasets() {
  const response = await fetch("http://localhost:8080/backend/api/datasets/");
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return response.json();
}

export default function ManagePage() {
  const { data, error, isLoading } = useQuery({
    queryKey: ["datasets"],
    queryFn: fetchDatasets,
  });

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error loading datasets</div>;

  console.log(data);

  return (
    <div>
      <main>
        <h1>Manage datasets</h1>

        <Link href="/manage/new">Add new dataset</Link>

        <h2>Existing datasets</h2>

        <ul>
          {data.map((dataset: { slug: string }) => (
            <li key={dataset.slug}>{dataset.slug}</li>
          ))}
        </ul>
      </main>
    </div>
  );
}
