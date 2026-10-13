import { Alert, Spinner } from "@chakra-ui/react";

import { errorMessage } from "src/queryClient";

export const QueryState = ({ error, isLoading }: { error: unknown; isLoading: boolean }) => {
  if (isLoading) return <Spinner />;
  if (!error) return undefined;
  return (
    <Alert.Root status="error">
      <Alert.Indicator />
      <Alert.Title>{errorMessage(error)}</Alert.Title>
    </Alert.Root>
  );
};
