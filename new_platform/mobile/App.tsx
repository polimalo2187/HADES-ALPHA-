import 'react-native-get-random-values';
import React, { useEffect, useState } from 'react';
import { StatusBar } from 'expo-status-bar';
import { SafeAreaProvider } from 'react-native-safe-area-context';
import { GestureHandlerRootView } from 'react-native-gesture-handler';
import { StyleSheet, View, ActivityIndicator, Text } from 'react-native';
import * as SplashScreen from 'expo-splash-screen';
import * as Font from 'expo-font';
import { THEME } from '@config/env';
import AppNavigator from '@navigation/AppNavigator';
import { api } from '@services/api';
import wsService from '@services/websocket';

// Mantener splash screen visible mientras cargamos recursos
SplashScreen.preventAutoHideAsync().catch(() => {});

export default function App() {
  const [appIsReady, setAppIsReady] = useState(false);

  useEffect(() => {
    async function prepare() {
      try {
        // Cargar fuentes
        await Font.loadAsync({
          // Aquí se pueden cargar fuentes personalizadas
        });

        // Verificar autenticación
        const isAuthenticated = await api.isAuthenticated();
        
        if (isAuthenticated) {
          // Reconectar WebSocket si hay tokens válidos
          // Esto se manejará en el contexto de autenticación
        }

        // Simular carga mínima para mejor UX
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (e) {
        console.warn('Error loading app:', e);
      } finally {
        setAppIsReady(true);
      }
    }

    prepare();
  }, []);

  const onLayoutRootView = async () => {
    if (appIsReady) {
      await SplashScreen.hideAsync();
    }
  };

  if (!appIsReady) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color={THEME.colors.primary} />
        <Text style={styles.loadingText}>Cargando HADES...</Text>
      </View>
    );
  }

  return (
    <GestureHandlerRootView style={styles.container} onLayout={onLayoutRootView}>
      <SafeAreaProvider>
        <StatusBar style="light" backgroundColor={THEME.colors.background} />
        <AppNavigator />
      </SafeAreaProvider>
    </GestureHandlerRootView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: THEME.colors.background,
  },
  loadingContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: THEME.colors.background,
  },
  loadingText: {
    color: THEME.colors.textSecondary,
    marginTop: 16,
    fontSize: 16,
    fontWeight: '600',
  },
});
