import React from 'react';
import { View, StyleSheet, ViewStyle, StyleProp } from 'react-native';
import { BlurView } from 'expo-blur';
import { THEME } from '@config/env';

interface GlassCardProps {
  children: React.ReactNode;
  style?: StyleProp<ViewStyle>;
  intensity?: number;
  tint?: 'light' | 'dark' | 'default';
  border?: boolean;
  glow?: boolean;
}

export const GlassCard: React.FC<GlassCardProps> = ({
  children,
  style,
  intensity = 60,
  tint = 'dark',
  border = true,
  glow = false,
}) => {
  return (
    <View style={[styles.container, style]}>
      <BlurView
        intensity={intensity}
        tint={tint}
        style={[
          styles.blurView,
          border && styles.border,
          glow && styles.glow,
        ]}
      >
        {children}
      </BlurView>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    overflow: 'hidden',
    borderRadius: THEME.borderRadius.lg,
  },
  blurView: {
    padding: THEME.spacing.md,
    backgroundColor: THEME.colors.glass,
    borderRadius: THEME.borderRadius.lg,
  },
  border: {
    borderWidth: 1,
    borderColor: THEME.colors.glassBorder,
  },
  glow: {
    shadowColor: THEME.colors.primary,
    shadowOffset: { width: 0, height: 0 },
    shadowOpacity: 0.3,
    shadowRadius: 20,
    elevation: 10,
  },
});

export default GlassCard;
