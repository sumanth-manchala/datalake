import {
  Alert,
  Badge,
  Button,
  Checkbox,
  HStack,
  Heading,
  Link,
  Spinner,
  Table,
  Text,
} from "@chakra-ui/react";
import { Link as RouterLink, useSearchParams } from "react-router-dom";

import { $api, errorMessage } from "src/queryClient";

export const DatasetList = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const showDropped = searchParams.get("dropped") === "1";
  const { data, error, isLoading } = $api.useQuery("get", "/v1/datasets");
  const rows = (data ?? []).filter((d) => showDropped || d.dropped_at === null);

  return (
    <>
      <HStack justify="space-between" mb={4}>
        <Heading size="lg">Datasets</Heading>
        <HStack gap={4}>
          <Checkbox.Root
            checked={showDropped}
            onCheckedChange={(e) => setSearchParams(e.checked ? { dropped: "1" } : {})}
          >
            <Checkbox.HiddenInput />
            <Checkbox.Control />
            <Checkbox.Label>Show dropped</Checkbox.Label>
          </Checkbox.Root>
          <Button asChild>
            <RouterLink to="/datasets/new">New dataset</RouterLink>
          </Button>
        </HStack>
      </HStack>
      {isLoading ? <Spinner /> : undefined}
      {error ? (
        <Alert.Root status="error">
          <Alert.Indicator />
          <Alert.Title>{errorMessage(error)}</Alert.Title>
        </Alert.Root>
      ) : undefined}
      {data && rows.length === 0 ? <Text color="fg.muted">No datasets yet.</Text> : undefined}
      {rows.length > 0 ? (
        <Table.Root size="sm">
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Dataset</Table.ColumnHeader>
              <Table.ColumnHeader>Version</Table.ColumnHeader>
              <Table.ColumnHeader>Partitioned by</Table.ColumnHeader>
              <Table.ColumnHeader>Warehouses</Table.ColumnHeader>
              <Table.ColumnHeader>Created</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {rows.map((d) => (
              <Table.Row key={`${d.namespace}.${d.name}.${d.version}`}>
                <Table.Cell>
                  <Link asChild>
                    <RouterLink to={`/datasets/${d.namespace}/${d.name}?version=${d.version}`}>
                      {d.namespace}.{d.name}
                    </RouterLink>
                  </Link>
                  {d.dropped_at ? (
                    <Badge ml={2} colorPalette="red">
                      dropped
                    </Badge>
                  ) : undefined}
                </Table.Cell>
                <Table.Cell>v{d.version}</Table.Cell>
                <Table.Cell>{d.partition_by.join(", ")}</Table.Cell>
                <Table.Cell>{d.warehouses.join(", ")}</Table.Cell>
                <Table.Cell>
                  {new Date(d.created_at).toLocaleString()} · {d.created_by}
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      ) : undefined}
    </>
  );
};
