"use client";
import type { IFormValues } from "@axdspub/axiom-ui-forms";
import { useQuery } from "@tanstack/react-query";
import type { JSONSchema6 } from "json-schema";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { use, useEffect, useState } from "react";

const Form = dynamic(() => import("./form"), { ssr: false });

interface Dataset {
  slug: string;
  pipeline: number;
  config: object;
  created_at: string;
  updated_at: string;
}

interface Pipeline {
  id: number;
  slug: string;
  config_schema: object;
  description: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

async function fetchDataset(slug: string): Promise<Dataset> {
  const response = await fetch(
    `http://localhost:8080/backend/api/datasets/${slug}`,
  );
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return (await response.json()) as Dataset;
}

async function fetchPipeline(id: number): Promise<Pipeline> {
  const response = await fetch(
    `http://localhost:8080/backend/api/pipelines/${id}`,
  );
  if (!response.ok) {
    throw new Error("Network response was not ok");
  }
  return (await response.json()) as Pipeline;
}

export default function Dataset({ params }: { params: { slug: string } }) {
  const { slug } = use(params);
  const router = useRouter();
  const { data, error, isLoading, refetch } = useQuery({
    queryKey: ["dataset", slug],
    queryFn: () => fetchDataset(slug),
  });

  const pipelineId = data?.pipeline;

  const {
    data: pipelineData,
    error: pipelineError,
    isLoading: pipelineLoading,
  } = useQuery({
    queryKey: ["pipeline", pipelineId],
    queryFn: () => fetchPipeline(pipelineId),
    enabled: !!pipelineId, // Only run this query if pipelineId is available
  });

  const [formValues, setFormValues] = useState<IFormValues>({});
  useEffect(() => {
    if (data?.config) {
      setFormValues(data.config as IFormValues);
    }
  }, [data]);

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;

  if (pipelineLoading) return <div>Loading pipeline...</div>;
  if (pipelineError)
    return <div>Error loading pipeline: {pipelineError.message}</div>;

  async function updateDataset() {
    const response = await fetch(
      `http://localhost:8080/backend/api/datasets/${slug}`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          config: formValues,
        }),
      },
    );

    if (!response.ok) {
      throw new Error("Failed to update dataset");
    }

    refetch();
    router.push("/manage/");
  }

  return (
    <div>
      <main>
        <h1>Dataset: {slug}</h1>
        Values: {JSON.stringify(formValues)}
        {pipelineData?.config_schema && data?.config ? (
          <form action={updateDataset}>
            <Form
              schema={pipelineData?.config_schema as JSONSchema6}
              formValueState={[formValues, setFormValues]}
            />
            <button type="submit">Update dataset</button>
          </form>
        ) : null}
      </main>
    </div>
  );
}
