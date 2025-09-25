"use client";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";

interface Pipeline {
  id: number;
  slug: string;
  config_schema: object;
  description: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

async function fetchPipelines(): Promise<Pipeline[]> {
  const response = await fetch("http://localhost:8080/backend/api/pipelines/");
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return response.json();
}

export default function NewDataset() {
  const router = useRouter();
  const { data, error, isLoading } = useQuery({
    queryKey: ["pipelines"],
    queryFn: fetchPipelines,
  });

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error loading pipelines</div>;

  console.log(data);

  async function newDataset(formData: FormData) {
    const dataset = {
      slug: formData.get("slug"),
      runner_id: formData.get("pipeline"),
      config: {},
    };

    const response = await fetch(
      "http://localhost:8080/backend/api/datasets/",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(dataset),
      },
    );

    if (!response.ok) {
      throw new Error("Failed to create dataset");
    }

    const newDataset = await response.json();
    console.log(newDataset);

    router.push(`/manage/dataset/${newDataset.slug}`);
  }

  return (
    <div>
      <main>
        <h1>New dataset</h1>

        <form action={newDataset}>
          <label htmlFor="slug">Unique dataset slug: </label>
          <input
            type="text"
            name="slug"
            placeholder="empire_wind_met"
            required
          />
          <label htmlFor="pipeline">Processing pipeline: </label>
          <select name="pipeline">
            {data.map((pipeline: Pipeline) => (
              <option key={pipeline.id} value={pipeline.id}>
                {pipeline.slug}
              </option>
            ))}
          </select>
          <button type="submit">Create dataset</button>
        </form>
      </main>
    </div>
  );
}
