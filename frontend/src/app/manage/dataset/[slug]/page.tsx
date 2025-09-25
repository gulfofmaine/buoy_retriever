"use client";
import {
  FormCreator,
  type IForm,
  type IFormValues,
  schemaToFormUtils,
} from "@axdspub/axiom-ui-forms";
import { useQuery } from "@tanstack/react-query";
import type { JSONSchema6 } from "json-schema";
import { use, useState } from "react";

interface Dataset {
  slug: string;
  runner: number;
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
  const { data, error, isLoading } = useQuery({
    queryKey: ["dataset", slug],
    queryFn: () => fetchDataset(slug),
  });

  const pipelineId = data?.runner;

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

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;

  if (pipelineLoading) return <div>Loading pipeline...</div>;
  if (pipelineError)
    return <div>Error loading pipeline: {pipelineError.message}</div>;

  console.log(data);
  console.log(pipelineData);
  const formConfig = schemaToFormUtils.schemaToFormObject(
    pipelineData?.config_schema as JSONSchema6,
  );
  console.log(formConfig);

  //   debugger;

  return (
    <div>
      <main>
        <h1>Dataset: {slug}</h1>
        Config: <code>{JSON.stringify(data.config)}</code>
        <FormCreator
          from={formConfig as IForm}
          formValues={[formValues, setFormValues]}
          urlNavigable={false}
        />
      </main>
    </div>
  );
}
