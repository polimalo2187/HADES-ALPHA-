export type RootStackParamList = {
  Auth: undefined;
  App: undefined;
};

export type AuthStackParamList = {
  Login: undefined;
  Register: undefined;
};

export type AppStackParamList = {
  MainTabs: undefined;
  SignalDetail: { signalId: string };
  PaymentDetail: { paymentId: string };
  Settings: undefined;
};

export type MainTabParamList = {
  Dashboard: undefined;
  Scanner: undefined;
  Market: { symbol?: string };
  Wallet: undefined;
  Profile: undefined;
};

export interface NavigationProps<T extends keyof any> {
  navigation: any;
  route: {
    name: T;
    params: any;
  };
}
