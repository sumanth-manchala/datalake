import { createBrowserRouter, Navigate } from "react-router-dom";

import { BaseLayout } from "src/layouts/BaseLayout";
import { CreateDataset } from "src/pages/CreateDataset";
import { DatasetDetail } from "src/pages/DatasetDetail";
import { DatasetList } from "src/pages/DatasetList";

export const routes = [
  {
    element: <BaseLayout />,
    children: [
      { path: "/", element: <Navigate to="/datasets" replace /> },
      { path: "/datasets", element: <DatasetList /> },
      { path: "/datasets/new", element: <CreateDataset /> },
      { path: "/datasets/:namespace/:name", element: <DatasetDetail /> },
    ],
  },
];

export const router = createBrowserRouter(routes);
