import { Alert, Button, NativeSelect } from "@chakra-ui/react";
import { useSearchParams } from "react-router-dom";

import type { DatasetRef } from "./index";
import { opLabel, useTimeTravelPoints } from "./ops";

/** Time-travel point, kept in the URL as ?as_of=<op_id>. */
export const useAsOf = () => useSearchParams()[0].get("as_of") ?? undefined;

export const AsOfPicker = ({ dataset }: { dataset: DatasetRef }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const asOf = searchParams.get("as_of") ?? "";
  const { data: points = [] } = useTimeTravelPoints(dataset);
  const selected = points.find((op) => op.op_id === asOf);

  const setAsOf = (value: string) =>
    setSearchParams((prev) => {
      if (value) prev.set("as_of", value);
      else prev.delete("as_of");
      return prev;
    });

  return (
    <>
      <NativeSelect.Root maxW="xl">
        <NativeSelect.Field
          aria-label="View as of"
          value={asOf}
          onChange={(e) => setAsOf(e.target.value)}
        >
          <option value="">Current</option>
          {/* a link may pin an id we don't list (e.g. a raw snapshot id); keep it selectable */}
          {asOf && !selected ? <option value={asOf}>{asOf}</option> : undefined}
          {points.map((op) => (
            <option key={op.op_id} value={op.op_id}>
              {opLabel(op)}
            </option>
          ))}
        </NativeSelect.Field>
        <NativeSelect.Indicator />
      </NativeSelect.Root>
      {asOf ? (
        <Alert.Root status="info">
          <Alert.Indicator />
          <Alert.Title>Viewing as of {selected ? opLabel(selected) : asOf}</Alert.Title>
          <Button size="xs" variant="outline" ms="auto" onClick={() => setAsOf("")}>
            Back to current
          </Button>
        </Alert.Root>
      ) : undefined}
    </>
  );
};
