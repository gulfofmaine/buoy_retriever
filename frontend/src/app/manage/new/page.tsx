"use client";
import { useRouter } from "next/navigation";

import { type Pipeline, usePipelines } from "@/hooks/queries";

export default function NewDataset() {
  const router = useRouter();
  const { data, isError, isPending } = usePipelines();

  if (isPending) return <div>Loading...</div>;
  if (isError) return <div>Error loading pipelines</div>;

  async function newDataset(formData: FormData) {
    const dataset = {
      slug: formData.get("slug"),
      pipeline_id: formData.get("pipeline"),
      config: {},
    };

    const response = await fetch("/backend/api/datasets/", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(dataset),
    });

    if (!response.ok) {
      throw new Error("Failed to create dataset");
    }

    const newDataset = await response.json();

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
