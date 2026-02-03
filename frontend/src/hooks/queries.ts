"use client";
import { skipToken, useQuery } from "@tanstack/react-query";

// Encode the current path to return to after login
function next(): string {
  return `?next=${encodeURIComponent(window.location.pathname)}`;
}

// Proxied path to the login page
export function loginUrl(): string {
  return `/backend/login/${next()}`;
}

// Fetch wrapper that handles 401 Unauthorized responses by redirecting to login
export async function fetchWithErrorHandling<T>(url: string) {
  const response = await fetch(url);

  // Redirect to login if unauthorized
  if (response.status === 401) {
    const nextUrl = loginUrl();

    window.location.href = nextUrl;
  }

  if (!response.ok) {
    throw new Error("Network response was not ok");
  }

  return (await response.json()) as T;
}

export interface DatasetCompact {
  slug: string;
  state: string;
  created: string;
  edited: string;
  user_can_edit: boolean;
  user_can_publish: boolean;
}

// Fetch list of datasets with permissions
export function useDatasets() {
  return useQuery({
    queryKey: ["datasets"],
    queryFn: () =>
      fetchWithErrorHandling<DatasetCompact[]>("/backend/api/datasets/"),
  });
}

export interface DatasetConfigCompact {
  config: object;
  created: string;
  edited: string;
  state: string;
  id: number;
}

export interface Dataset {
  id: number;
  slug: string;
  pipeline: number;
  configs: DatasetConfigCompact[];
  created: string;
  edited: string;
  state: string;
}

// Fetch a specific dataset by slug
export function useDataset(slug: string) {
  return useQuery({
    queryKey: ["dataset", slug],
    queryFn: () =>
      fetchWithErrorHandling<Dataset>(`/backend/api/datasets/${slug}/`),
  });
}

export interface Pipeline {
  id: number;
  slug: string;
  config_schema: object;
  description: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

// Fetch a specific pipeline by ID
export function usePipeline(id: number | undefined) {
  return useQuery({
    queryKey: ["pipeline", id],
    queryFn: id
      ? () => fetchWithErrorHandling<Pipeline>(`/backend/api/pipelines/${id}/`)
      : skipToken,
  });
}

// Fetch list of pipelines
export function usePipelines() {
  return useQuery({
    queryKey: ["pipelines"],
    queryFn: () =>
      fetchWithErrorHandling<Pipeline[]>("/backend/api/pipelines/"),
  });
}
