import React from 'react';
import { NavigationContainer } from '@react-navigation/native';
import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { RootStackParamList, AuthStackParamList, AppStackParamList } from './types';
import { LoginScreen } from '@screens/LoginScreen';
import BottomTabNavigator from './BottomTabNavigator';

// Placeholder para RegisterScreen
const RegisterScreen = () => null;

const Stack = createNativeStackNavigator<RootStackParamList>();
const AuthStack = createNativeStackNavigator<AuthStackParamList>();
const AppStack = createNativeStackNavigator<AppStackParamList>();

const AuthNavigator = () => (
  <AuthStack.Navigator
    screenOptions={{
      headerShown: false,
      contentStyle: { backgroundColor: '#0a0a0f' },
    }}
  >
    <AuthStack.Screen name="Login" component={LoginScreen} />
    <AuthStack.Screen name="Register" component={RegisterScreen} />
  </AuthStack.Navigator>
);

const AppNavigator = () => (
  <AppStack.Navigator
    screenOptions={{
      headerShown: false,
      contentStyle: { backgroundColor: '#0a0a0f' },
    }}
  >
    <AppStack.Screen name="MainTabs" component={BottomTabNavigator} />
  </AppStack.Navigator>
);

export const AppNavigatorRoot = () => {
  // Aquí se verificará el estado de autenticación
  // Por ahora mostramos AuthNavigator por defecto
  return (
    <NavigationContainer>
      <AuthNavigator />
    </NavigationContainer>
  );
};

export default AppNavigatorRoot;
