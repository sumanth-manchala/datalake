import { Box, Container, HStack, Heading, Link } from "@chakra-ui/react";
import { Link as RouterLink, Outlet } from "react-router-dom";

export const BaseLayout = () => (
  <Box minH="100vh" bg="bg">
    <Box as="header" borderBottomWidth="1px" borderColor="border" py={3}>
      <Container maxW="6xl">
        <HStack gap={6}>
          <Heading size="md">Datalake</Heading>
          <Link asChild color="fg.muted">
            <RouterLink to="/datasets">Datasets</RouterLink>
          </Link>
        </HStack>
      </Container>
    </Box>
    <Container as="main" maxW="6xl" py={6}>
      <Outlet />
    </Container>
  </Box>
);
