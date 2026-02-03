"use client";
import {
  FormCreator,
  type IForm,
  type IFormValues,
  schemaToFormUtils,
} from "@axdspub/axiom-ui-forms";
import type { JSONSchema6 } from "json-schema";

export default function Form({
  schema,
  formValueState,
}: {
  schema: JSONSchema6;
  formValueState: [
    IFormValues,
    React.Dispatch<React.SetStateAction<IFormValues>>,
  ];
}) {
  const formConfig = schemaToFormUtils.schemaToFormObject(schema);

  return (
    <FormCreator
      form={formConfig as IForm}
      formValueState={formValueState}
      urlNavigable={false}
    />
  );
}
