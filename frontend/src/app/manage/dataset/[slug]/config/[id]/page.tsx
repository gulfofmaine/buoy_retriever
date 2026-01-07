"use client";
import type { IFormValues } from "@axdspub/axiom-ui-forms";
import type { JSONSchema6 } from "json-schema";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import { use, useEffect, useState } from "react";

import { useDataset, usePipeline } from "@/hooks/queries";

const Form = dynamic(() => import("./form"), { ssr: false });

export default function Config({
  params,
}: {
  params: Promise<{ slug: string; id: string }>;
}) {
  const { slug, id } = use(params);
  const router = useRouter();
  const { data, error, isLoading, refetch } = useDataset(slug);

  const pipelineId = data?.pipeline;

  const {
    data: pipelineData,
    error: pipelineError,
    isLoading: pipelineLoading,
  } = usePipeline(pipelineId);

  const [formValues, setFormValues] = useState<IFormValues>({});
  useEffect(() => {
    if (data) {
      const config = data.configs.find((c) => c.id.toString() === id);
      if (config) {
        setFormValues(config.config as IFormValues);
      }
    }
  }, [data, id]);

  if (isLoading || pipelineLoading) return <div>Loading...</div>;
  if (error) return <div>Error loading dataset</div>;
  if (pipelineError) return <div>Error loading pipeline</div>;

  async function updateConfig() {
    const response = await fetch(`/backend/api/configs/${id}/`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        config: formValues,
      }),
    });

    if (!response.ok) {
      throw new Error("Failed to update the config");
    }

    refetch();
    router.push(`/manage/dataset/${slug}`);
  }

  return (
    <div>
      <main>
        <h1>
          Config {id} for dataset {slug}
        </h1>
        Values: {JSON.stringify(formValues)}
        {pipelineData?.config_schema && formValues ? (
          <>
            {/* <form
         action={updateConfig}
           > */}
            {pipelineData.slug}
            <Form
              schema={pipelineData.config_schema as JSONSchema6}
              formValueState={[formValues, setFormValues]}
            />
            {/* <button type="submit">Update config</button> */}
            <button type="button" onClick={updateConfig}>
              Update config
            </button>
            {/* </form> */}
          </>
        ) : null}
      </main>
    </div>
  );
}
