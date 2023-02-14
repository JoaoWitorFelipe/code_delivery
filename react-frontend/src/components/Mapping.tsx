import { Button, Grid, MenuItem, Select } from "@mui/material";
import { Loader } from "google-maps";
import { FormEvent, useCallback, useEffect, useRef, useState } from "react";
import { getCurrentPosition } from "../util/geolocation";
import { makeCarIcon, makeMarkerIcon, Map } from "../util/map";
import { Route } from "../util/models";
import _, { sample, shuffle } from "lodash";
import { RouteExistsError } from "../errors/route-exists";
import { useSnackbar } from "notistack";
import { Navbar } from "./Navbar";
import io, { Socket } from "socket.io-client";
import { RemoveRoadOutlined } from "@mui/icons-material";

type Props = {};

const API_URL = process.env.REACT_APP_API_URL as string;

const googleMapsLoader = new Loader(process.env.REACT_APP_GOOGLE_API_KEY);

const colors = [
  "#b71c1c",
  "#4a148c",
  "#2e7d32",
  "#e65100",
  "#2962ff",
  "#c2185b",
  "#FFCD00",
  "#3e2723",
  "#03a9f4",
  "#827717",
];

export const Mapping: React.FunctionComponent = (props: Props) => {
  const [routes, setRoutes] = useState<Route[]>([]);
  const [routeIdSelected, setRouteIdSelected] = useState<string>("");
  const mapRef = useRef<Map>();
  const socketIORef = useRef<Socket>();
  const { enqueueSnackbar } = useSnackbar();
  const finishRoute = useCallback(
    (route: Route) => {
      enqueueSnackbar(`${route.title} finalizou!`, {
        variant: "success",
      });
      mapRef.current?.removeRoute(route._id);
    },
    [enqueueSnackbar]
  );

  useEffect(() => {
    if (!socketIORef.current?.connected) {
      socketIORef.current = io(API_URL);
      socketIORef.current.on("connect", () => console.log("connect"));
    }

    const handler = (data: {
      routeId: string;
      position: [number, number];
      finished: boolean;
    }) => {
      mapRef.current?.moveCurrentMarker(data.routeId, {
        lat: data.position[0],
        lng: data.position[1],
      });

      const route = routes.find((route) => route._id === data.routeId) as Route;
      if (data.finished) {
        finishRoute(route);
      }
    };
    socketIORef.current?.on("new-position", handler);
    return () => {
      socketIORef.current?.off("new-position", handler);
    };
  }, [finishRoute, routes, routeIdSelected]);

  useEffect(() => {
    fetch(`${API_URL}/routes`)
      .then((data) => data.json())
      .then((data) => setRoutes(data));
  }, []);

  useEffect(() => {
    (async () => {
      const [, position] = await Promise.all([
        googleMapsLoader.load(),
        getCurrentPosition({
          enableHighAccuracy: true,
        }),
      ]);
      const divMap = document.getElementById("map") as HTMLElement;
      mapRef.current = new Map(divMap, {
        zoom: 15,
        center: position,
      });
    })();
  }, []);

  const startRoute = useCallback(
    (event: FormEvent) => {
      event.preventDefault();
      const route = routes.find((route) => route._id === routeIdSelected);
      const color = sample(shuffle(colors)) as string;

      try {
        mapRef.current?.addRoute(routeIdSelected, {
          currenctMarkerOptions: {
            position: route?.startPosition,
            icon: makeCarIcon(color),
          },
          endMarkerOptions: {
            position: route?.endPosition,
            icon: makeMarkerIcon(color),
          },
        });
        socketIORef.current?.emit("new-direction", {
          routeId: routeIdSelected,
        });
      } catch (error) {
        if (error instanceof RouteExistsError) {
          enqueueSnackbar(`${route?.title} já adicionado, espere finalizar`, {
            variant: "error",
          });
          return;
        }

        throw error;
      }
    },
    [routeIdSelected, routes, enqueueSnackbar]
  );

  return (
    <Grid container style={{ width: "100%", height: "100%" }}>
      <Grid item xs={12} sm={3}>
        <Navbar />
        <form onSubmit={startRoute} style={{ margin: "16px" }}>
          <Select
            fullWidth
            displayEmpty
            onChange={(event) => setRouteIdSelected(event.target.value + "")}
          >
            <MenuItem value={routeIdSelected}>
              <em>Selecione uma corrida</em>
            </MenuItem>

            {routes.map((route, key) => (
              <MenuItem key={key} value={route._id}>
                <em>{route.title}</em>
              </MenuItem>
            ))}
          </Select>
          <Button
            type="submit"
            color="primary"
            variant="contained"
            style={{ textAlign: "center", marginTop: "8px", width: "100%" }}
          >
            Iniciar corrida
          </Button>
        </form>
      </Grid>
      <Grid item xs={12} sm={9}>
        <div id="map" style={{ width: "100%", height: "100%" }}></div>
      </Grid>
    </Grid>
  );
};
