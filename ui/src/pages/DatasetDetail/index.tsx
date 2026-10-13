import { Badge, HStack, Heading, NativeSelect, Stack, Tabs, Text } from "@chakra-ui/react";
import { useParams, useSearchParams } from "react-router-dom";

import { $api } from "src/queryClient";

import { HistoryTab } from "./HistoryTab";
import { PartitionsTab } from "./PartitionsTab";
import { SchemaTab } from "./SchemaTab";

export type DatasetRef = { namespace: string; name: string; version: number | undefined };

export const DatasetDetail = () => {
  const { namespace = "", name = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get("tab") ?? "schema";
  const { data } = $api.useQuery("get", "/v1/datasets");

  const versions = (data ?? []).filter((d) => d.namespace === namespace && d.name === name);
  const versionParam = searchParams.get("version");
  const version = versionParam ? Number(versionParam) : versions.at(-1)?.version;
  const current = versions.find((d) => d.version === version);
  const ref: DatasetRef = { namespace, name, version };

  const setParam = (key: string, value: string) =>
    setSearchParams((prev) => {
      prev.set(key, value);
      return prev;
    });

  return (
    <Stack gap={4}>
      <HStack justify="space-between">
        <Heading size="lg">
          {namespace}.{name}
        </Heading>
        {versions.length > 1 ? (
          <NativeSelect.Root width="32">
            <NativeSelect.Field
              aria-label="Version"
              value={version}
              onChange={(e) => setParam("version", e.target.value)}
            >
              {versions.map((d) => (
                <option key={d.version} value={d.version}>
                  v{d.version}
                </option>
              ))}
            </NativeSelect.Field>
            <NativeSelect.Indicator />
          </NativeSelect.Root>
        ) : undefined}
      </HStack>
      {current ? (
        <HStack gap={3} wrap="wrap">
          <Text color="fg.muted">Partitioned by</Text>
          {current.partition_by.map((c) => (
            <Badge key={c}>{c}</Badge>
          ))}
          {current.dropped_at ? <Badge colorPalette="red">dropped</Badge> : undefined}
          {current.description ? <Text color="fg.muted">· {current.description}</Text> : undefined}
        </HStack>
      ) : undefined}
      <Tabs.Root value={tab} onValueChange={(e) => setParam("tab", e.value)} lazyMount>
        <Tabs.List>
          <Tabs.Trigger value="schema" disabled={!!current?.dropped_at}>
            Schema
          </Tabs.Trigger>
          <Tabs.Trigger value="partitions" disabled={!!current?.dropped_at}>
            Partitions
          </Tabs.Trigger>
          <Tabs.Trigger value="history">History</Tabs.Trigger>
        </Tabs.List>
        <Tabs.Content value="schema">
          <SchemaTab dataset={ref} />
        </Tabs.Content>
        <Tabs.Content value="partitions">
          <PartitionsTab dataset={ref} />
        </Tabs.Content>
        <Tabs.Content value="history">
          <HistoryTab dataset={ref} />
        </Tabs.Content>
      </Tabs.Root>
    </Stack>
  );
};
