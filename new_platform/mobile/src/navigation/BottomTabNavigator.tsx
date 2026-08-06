import React from 'react';
import { createBottomTabNavigator } from '@react-navigation/bottom-tabs';
import { BlurView } from 'expo-blur';
import { View, Text, StyleSheet, Platform } from 'react-native';
import { THEME } from '@config/env';

// Placeholder screens - se importarán de los archivos reales
const DashboardScreen = () => (
  <View style={styles.screen}><Text>Dashboard</Text></View>
);
const ScannerScreen = () => (
  <View style={styles.screen}><Text>Scanner</Text></View>
);
const MarketScreen = () => (
  <View style={styles.screen}><Text>Mercado</Text></View>
);
const WalletScreen = () => (
  <View style={styles.screen}><Text>Billetera</Text></View>
);
const ProfileScreen = () => (
  <View style={styles.screen}><Text>Perfil</Text></View>
);

const Tab = createBottomTabNavigator();

interface TabIconProps {
  focused: boolean;
  color: string;
  size: number;
  label: string;
}

const TabIcon: React.FC<TabIconProps> = ({ focused, color, size, label }) => (
  <View style={styles.tabIcon}>
    <Text style={[styles.tabIconText, { color, fontSize: size }]}>
      {label}
    </Text>
  </View>
);

export const BottomTabNavigator = () => {
  return (
    <Tab.Navigator
      screenOptions={({ route }) => ({
        headerShown: false,
        tabBarStyle: styles.tabBar,
        tabBarBackground: () => (
          <BlurView
            intensity={80}
            tint="dark"
            style={styles.blurBackground}
          />
        ),
        tabBarActiveTintColor: THEME.colors.primary,
        tabBarInactiveTintColor: THEME.colors.textMuted,
        tabBarLabelStyle: styles.tabBarLabel,
        tabBarItemStyle: styles.tabBarItem,
        tabBarIcon: ({ focused, color, size }) => {
          let label = '';
          switch (route.name) {
            case 'Dashboard':
              label = '📊';
              break;
            case 'Scanner':
              label = '🔍';
              break;
            case 'Market':
              label = '📈';
              break;
            case 'Wallet':
              label = '💰';
              break;
            case 'Profile':
              label = '👤';
              break;
          }
          return <TabIcon focused={focused} color={color} size={size} label={label} />;
        },
      })}
    >
      <Tab.Screen
        name="Dashboard"
        component={DashboardScreen}
        options={{ title: 'Inicio' }}
      />
      <Tab.Screen
        name="Scanner"
        component={ScannerScreen}
        options={{ title: 'Scanner' }}
      />
      <Tab.Screen
        name="Market"
        component={MarketScreen}
        options={{ title: 'Mercado' }}
      />
      <Tab.Screen
        name="Wallet"
        component={WalletScreen}
        options={{ title: 'Billetera' }}
      />
      <Tab.Screen
        name="Profile"
        component={ProfileScreen}
        options={{ title: 'Perfil' }}
      />
    </Tab.Navigator>
  );
};

const styles = StyleSheet.create({
  tabBar: {
    position: 'absolute',
    backgroundColor: 'transparent',
    borderTopWidth: 0,
    elevation: 0,
    shadowOpacity: 0,
    height: 80,
    paddingBottom: Platform.OS === 'ios' ? 20 : 10,
    paddingTop: 10,
  },
  blurBackground: {
    ...StyleSheet.absoluteFillObject,
    borderRadius: 20,
    overflow: 'hidden',
  },
  tabBarItem: {
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
  },
  tabBarLabel: {
    fontSize: 10,
    fontWeight: '600',
    marginTop: 4,
  },
  tabIcon: {
    alignItems: 'center',
    justifyContent: 'center',
  },
  tabIconText: {
    fontWeight: 'bold',
  },
  screen: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: THEME.colors.background,
  },
});

export default BottomTabNavigator;
